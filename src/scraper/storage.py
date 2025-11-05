"""Storage backends for scraped data."""
import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
import polars as pl
import aiofiles
from google.cloud import storage
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from .models import ScrapingResult, ScrapedContent, StorageConfig

logger = logging.getLogger(__name__)


class StorageBackend:
    """Base class for storage backends."""
    
    async def save_result(self, result: ScrapingResult) -> str:
        """Save scraping result and return storage path."""
        raise NotImplementedError
        
    async def save_pages(self, pages: List[ScrapedContent], website_id: str) -> str:
        """Save individual pages and return storage path."""
        raise NotImplementedError


class LocalStorageBackend(StorageBackend):
    """Local file system storage backend."""
    
    def __init__(self, config: StorageConfig):
        self.config = config
        self.output_dir = Path(config.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def _get_file_path(self, website_id: str, file_type: str = "parquet") -> Path:
        """Get file path for storing data."""
        if self.config.partition_by_date:
            date_str = datetime.utcnow().strftime("%Y_%m_%d")
            file_dir = self.output_dir / date_str
        else:
            file_dir = self.output_dir

        file_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{website_id}_{timestamp}.{file_type}"

        return file_dir / filename
        
    async def save_result(self, result: ScrapingResult) -> str:
        """Save complete scraping result as parquet file."""
        try:
            website_id = result.website_id or "unknown"
            file_path = self._get_file_path(website_id)

            # Convert to Polars DataFrame
            df_data = []
            for page in result.pages:
                row = {
                    "website_id": result.website_id,
                    "original_url": result.original_url,
                    "page_url": page.url,
                    "title": page.title,
                    "html": page.html,
                    "text": page.text,
                    "scraped_at": page.scraped_at,
                    "load_time": page.load_time,
                    "status_code": page.status_code,
                    "error": page.error,
                    "link_count": len(page.links),
                    "image_count": len(page.images),
                    "content_length": len(page.html),
                    "text_length": len(page.text)
                }

                # Add metadata as separate columns
                for key, value in page.metadata.items():
                    row[f"metadata_{key}"] = value

                df_data.append(row)

            df = pl.DataFrame(df_data)

            # Save as parquet
            df.write_parquet(
                file_path,
                compression=self.config.compression
            )

            logger.info(f"Saved scraping result to {file_path}")
            return str(file_path)

        except Exception as e:
            logger.error(f"Failed to save result locally: {e}")
            raise
            
    async def save_pages(self, pages: List[ScrapedContent], website_id: str) -> str:
        """Save individual pages as separate files."""
        try:
            base_path = self._get_file_path(website_id).parent / website_id
            base_path.mkdir(parents=True, exist_ok=True)
            
            saved_files = []
            for i, page in enumerate(pages):
                # Save HTML
                html_file = base_path / f"page_{i:03d}.html"
                async with aiofiles.open(html_file, 'w', encoding='utf-8') as f:
                    await f.write(page.html)
                    
                # Save text
                text_file = base_path / f"page_{i:03d}.txt"
                async with aiofiles.open(text_file, 'w', encoding='utf-8') as f:
                    await f.write(page.text)
                    
                saved_files.extend([str(html_file), str(text_file)])
                
            logger.info(f"Saved {len(pages)} pages to {base_path}")
            return str(base_path)
            
        except Exception as e:
            logger.error(f"Failed to save pages locally: {e}")
            raise


class CloudStorageBackend(StorageBackend):
    """Google Cloud Storage backend."""

    def __init__(self, config: StorageConfig):
        self.config = config
        if not config.bucket_name:
            raise ValueError("bucket_name is required for cloud storage")

        # Initialize client with credentials file priority
        if config.gcs_credentials_file:
            self.client = storage.Client.from_service_account_json(config.gcs_credentials_file)
        else:
            # Fall back to default credentials (environment variables, etc.)
            self.client = storage.Client()

        self.bucket = self.client.bucket(config.bucket_name)
        
    def _get_blob_path(self, website_id: str, file_type: str = "parquet") -> str:
        """Get blob path for storing data."""
        if self.config.partition_by_date:
            date_str = datetime.utcnow().strftime("%Y_%m_%d")
            path_prefix = f"data/{date_str}"
        else:
            path_prefix = "data"

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{website_id}_{timestamp}.{file_type}"

        return f"{path_prefix}/{filename}"
        
    async def save_result(self, result: ScrapingResult) -> str:
        """Save complete scraping result to cloud storage."""
        try:
            website_id = result.website_id or "unknown"
            blob_path = self._get_blob_path(website_id)

            # Convert to Polars DataFrame
            df_data = []
            for page in result.pages:
                row = {
                    "website_id": result.website_id,
                    "original_url": result.original_url,
                    "page_url": page.url,
                    "title": page.title,
                    "html": page.html,
                    "text": page.text,
                    "scraped_at": page.scraped_at,
                    "load_time": page.load_time,
                    "status_code": page.status_code,
                    "error": page.error,
                    "link_count": len(page.links),
                    "image_count": len(page.images),
                    "content_length": len(page.html),
                    "text_length": len(page.text)
                }

                # Add metadata as separate columns
                for key, value in page.metadata.items():
                    row[f"metadata_{key}"] = value

                df_data.append(row)

            df = pl.DataFrame(df_data)

            # Save to temporary file first
            temp_file = f"/tmp/{website_id}_{datetime.utcnow().isoformat()}.parquet"
            df.write_parquet(
                temp_file,
                compression=self.config.compression
            )

            # Upload to cloud storage
            def _upload():
                blob = self.bucket.blob(blob_path)
                blob.upload_from_filename(temp_file)

            await asyncio.get_event_loop().run_in_executor(None, _upload)

            # Clean up temp file
            os.unlink(temp_file)

            logger.info(f"Saved scraping result to gs://{self.config.bucket_name}/{blob_path}")
            return f"gs://{self.config.bucket_name}/{blob_path}"

        except Exception as e:
            logger.error(f"Failed to save result to cloud storage: {e}")
            raise
            
    async def save_pages(self, pages: List[ScrapedContent], website_id: str) -> str:
        """Save individual pages to cloud storage."""
        try:
            base_path = f"scraped_pages/{website_id}"
            if self.config.partition_by_date:
                date_str = datetime.utcnow().strftime("%Y/%m/%d")
                base_path = f"{base_path}/{date_str}"
                
            saved_files = []
            
            def _upload_content(content: str, blob_path: str):
                blob = self.bucket.blob(blob_path)
                blob.upload_from_string(content, content_type='text/html')
                
            # Upload all pages concurrently
            upload_tasks = []
            for i, page in enumerate(pages):
                # HTML file
                html_path = f"{base_path}/page_{i:03d}.html"
                upload_tasks.append(
                    asyncio.get_event_loop().run_in_executor(
                        None, _upload_content, page.html, html_path
                    )
                )
                
                # Text file
                text_path = f"{base_path}/page_{i:03d}.txt"
                upload_tasks.append(
                    asyncio.get_event_loop().run_in_executor(
                        None, _upload_content, page.text, text_path
                    )
                )
                
                saved_files.extend([html_path, text_path])
                
            await asyncio.gather(*upload_tasks)
            
            logger.info(f"Saved {len(pages)} pages to gs://{self.config.bucket_name}/{base_path}")
            return f"gs://{self.config.bucket_name}/{base_path}"
            
        except Exception as e:
            logger.error(f"Failed to save pages to cloud storage: {e}")
            raise


class S3StorageBackend(StorageBackend):
    """AWS S3 Storage backend."""

    def __init__(self, config: StorageConfig):
        self.config = config
        if not config.bucket_name:
            raise ValueError("bucket_name is required for S3 storage")

        # Initialize S3 client with credentials file priority
        session_kwargs = {}
        if config.aws_credentials_file:
            # Load credentials from JSON file
            with open(config.aws_credentials_file, 'r') as f:
                creds = json.load(f)
                session_kwargs['aws_access_key_id'] = creds.get('aws_access_key_id')
                session_kwargs['aws_secret_access_key'] = creds.get('aws_secret_access_key')
                session_kwargs['region_name'] = creds.get('region', config.aws_region)
        elif config.aws_access_key_id and config.aws_secret_access_key:
            # Use credentials from config
            session_kwargs['aws_access_key_id'] = config.aws_access_key_id
            session_kwargs['aws_secret_access_key'] = config.aws_secret_access_key
            session_kwargs['region_name'] = config.aws_region
        else:
            # Fall back to default credentials (environment variables, ~/.aws/credentials, etc.)
            session_kwargs['region_name'] = config.aws_region

        self.s3_client = boto3.client('s3', **session_kwargs)
        self.bucket_name = config.bucket_name

    def _get_s3_key(self, website_id: str, file_type: str = "parquet") -> str:
        """Get S3 key for storing data."""
        if self.config.partition_by_date:
            date_str = datetime.utcnow().strftime("%Y_%m_%d")
            path_prefix = f"data/{date_str}"
        else:
            path_prefix = "data"

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{website_id}_{timestamp}.{file_type}"

        return f"{path_prefix}/{filename}"

    async def save_result(self, result: ScrapingResult) -> str:
        """Save complete scraping result to S3."""
        try:
            website_id = result.website_id or "unknown"
            s3_key = self._get_s3_key(website_id)

            # Convert to Polars DataFrame
            df_data = []
            for page in result.pages:
                row = {
                    "website_id": result.website_id,
                    "original_url": result.original_url,
                    "page_url": page.url,
                    "title": page.title,
                    "html": page.html,
                    "text": page.text,
                    "scraped_at": page.scraped_at,
                    "load_time": page.load_time,
                    "status_code": page.status_code,
                    "error": page.error,
                    "link_count": len(page.links),
                    "image_count": len(page.images),
                    "content_length": len(page.html),
                    "text_length": len(page.text)
                }

                # Add metadata as separate columns
                for key, value in page.metadata.items():
                    row[f"metadata_{key}"] = value

                df_data.append(row)

            df = pl.DataFrame(df_data)

            # Save to temporary file first
            temp_file = f"/tmp/{website_id}_{datetime.utcnow().isoformat()}.parquet"
            df.write_parquet(
                temp_file,
                compression=self.config.compression
            )

            # Upload to S3
            def _upload():
                self.s3_client.upload_file(temp_file, self.bucket_name, s3_key)

            await asyncio.get_event_loop().run_in_executor(None, _upload)

            # Clean up temp file
            os.unlink(temp_file)

            logger.info(f"Saved scraping result to s3://{self.bucket_name}/{s3_key}")
            return f"s3://{self.bucket_name}/{s3_key}"

        except Exception as e:
            logger.error(f"Failed to save result to S3: {e}")
            raise

    async def save_pages(self, pages: List[ScrapedContent], website_id: str) -> str:
        """Save individual pages to S3."""
        try:
            if self.config.partition_by_date:
                date_str = datetime.utcnow().strftime("%Y_%m_%d")
                base_path = f"pages/{website_id}/{date_str}"
            else:
                base_path = f"pages/{website_id}"

            saved_files = []

            def _upload_content(content: str, s3_key: str, content_type: str):
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=content.encode('utf-8'),
                    ContentType=content_type
                )

            # Upload all pages concurrently
            upload_tasks = []
            for i, page in enumerate(pages):
                # HTML file
                html_key = f"{base_path}/page_{i:03d}.html"
                upload_tasks.append(
                    asyncio.get_event_loop().run_in_executor(
                        None, _upload_content, page.html, html_key, 'text/html'
                    )
                )

                # Text file
                text_key = f"{base_path}/page_{i:03d}.txt"
                upload_tasks.append(
                    asyncio.get_event_loop().run_in_executor(
                        None, _upload_content, page.text, text_key, 'text/plain'
                    )
                )

                saved_files.extend([html_key, text_key])

            await asyncio.gather(*upload_tasks)

            logger.info(f"Saved {len(pages)} pages to s3://{self.bucket_name}/{base_path}")
            return f"s3://{self.bucket_name}/{base_path}"

        except Exception as e:
            logger.error(f"Failed to save pages to S3: {e}")
            raise


class ResilientStorageBackend(StorageBackend):
    """Storage backend with automatic fallback to local storage."""

    def __init__(self, primary_backend: StorageBackend, fallback_backend: LocalStorageBackend):
        self.primary_backend = primary_backend
        self.fallback_backend = fallback_backend
        self.has_fallen_back = False

    async def save_result(self, result: ScrapingResult) -> str:
        """Save result with automatic fallback to local storage on cloud failure."""
        try:
            return await self.primary_backend.save_result(result)
        except Exception as e:
            if not self.has_fallen_back:
                logger.warning(
                    f"Cloud storage failed ({type(e).__name__}: {e}), "
                    f"falling back to local storage"
                )
                self.has_fallen_back = True
            return await self.fallback_backend.save_result(result)

    async def save_pages(self, pages: List[ScrapedContent], website_id: str) -> str:
        """Save pages with automatic fallback to local storage on cloud failure."""
        try:
            return await self.primary_backend.save_pages(pages, website_id)
        except Exception as e:
            if not self.has_fallen_back:
                logger.warning(
                    f"Cloud storage failed ({type(e).__name__}: {e}), "
                    f"falling back to local storage"
                )
                self.has_fallen_back = True
            return await self.fallback_backend.save_pages(pages, website_id)


def get_storage_backend(config: StorageConfig) -> StorageBackend:
    """Factory function to get appropriate storage backend.

    Args:
        config: Storage configuration

    Returns:
        StorageBackend instance (with optional fallback wrapper)

    Raises:
        ValueError: If storage_type is invalid or required config is missing
    """
    storage_type = config.storage_type.lower()

    # Local storage - no fallback needed
    if storage_type == "local":
        return LocalStorageBackend(config)

    # Cloud storage types
    elif storage_type == "s3":
        primary = S3StorageBackend(config)
    elif storage_type == "gcs":
        primary = CloudStorageBackend(config)
    else:
        raise ValueError(
            f"Invalid storage_type: {config.storage_type}. "
            f"Must be 'local', 's3', or 'gcs'"
        )

    # Wrap cloud storage with fallback if enabled
    if config.enable_fallback:
        fallback = LocalStorageBackend(config)
        return ResilientStorageBackend(primary, fallback)

    return primary