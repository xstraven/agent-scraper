"""Storage backends for scraped data."""
import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
import pandas as pd
import aiofiles
from google.cloud import storage

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
            date_str = datetime.utcnow().strftime("%Y/%m/%d")
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
            
            # Convert to DataFrame
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
                
            df = pd.DataFrame(df_data)
            
            # Save as parquet
            df.to_parquet(
                file_path,
                compression=self.config.compression,
                index=False
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
    
    def __init__(self, config: StorageConfig, credentials_path: Optional[str] = None):
        self.config = config
        if not config.bucket_name:
            raise ValueError("bucket_name is required for cloud storage")
            
        # Initialize client
        if credentials_path:
            self.client = storage.Client.from_service_account_json(credentials_path)
        else:
            self.client = storage.Client()
            
        self.bucket = self.client.bucket(config.bucket_name)
        
    def _get_blob_path(self, website_id: str, file_type: str = "parquet") -> str:
        """Get blob path for storing data."""
        if self.config.partition_by_date:
            date_str = datetime.utcnow().strftime("%Y/%m/%d")
            path_prefix = f"scraped_data/{date_str}"
        else:
            path_prefix = "scraped_data"
            
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{website_id}_{timestamp}.{file_type}"
        
        return f"{path_prefix}/{filename}"
        
    async def save_result(self, result: ScrapingResult) -> str:
        """Save complete scraping result to cloud storage."""
        try:
            website_id = result.website_id or "unknown"
            blob_path = self._get_blob_path(website_id)
            
            # Convert to DataFrame
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
                
            df = pd.DataFrame(df_data)
            
            # Save to temporary file first
            temp_file = f"/tmp/{website_id}_{datetime.utcnow().isoformat()}.parquet"
            df.to_parquet(
                temp_file,
                compression=self.config.compression,
                index=False
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


def get_storage_backend(config: StorageConfig, credentials_path: Optional[str] = None) -> StorageBackend:
    """Factory function to get appropriate storage backend."""
    if config.use_cloud_storage:
        return CloudStorageBackend(config, credentials_path)
    else:
        return LocalStorageBackend(config)