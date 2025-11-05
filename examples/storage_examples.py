"""
Examples of using different storage backends with the scraper.

This file demonstrates:
1. Local storage (default)
2. AWS S3 storage with credentials file
3. AWS S3 storage with explicit credentials
4. Google Cloud Storage with credentials file
5. Storage with fallback enabled/disabled
"""

import asyncio
from scraper.models import StorageConfig, ScrapingRequest
from scraper.scraper import WebsiteScraper


async def example_local_storage():
    """Example: Local storage (default)"""
    print("=" * 60)
    print("Example 1: Local Storage (Default)")
    print("=" * 60)

    # Default configuration - saves to data/raw/YYYY_MM_DD/
    config = StorageConfig()

    scraper = WebsiteScraper(storage_config=config)

    request = ScrapingRequest(
        url="https://example.com",
        website_id="example",
        max_pages=1
    )

    result = await scraper.scrape_website(request)
    print(f"Data saved to: {result}")
    print()


async def example_s3_with_credentials_file():
    """Example: AWS S3 storage with credentials file"""
    print("=" * 60)
    print("Example 2: AWS S3 Storage with Credentials File")
    print("=" * 60)

    # S3 storage with credentials from JSON file
    config = StorageConfig(
        storage_type="s3",
        bucket_name="my-scraper-bucket",
        aws_credentials_file="aws_credentials.json",
        partition_by_date=True,
        enable_fallback=True  # Falls back to local if S3 fails
    )

    scraper = WebsiteScraper(storage_config=config)

    request = ScrapingRequest(
        url="https://example.com",
        website_id="example",
        max_pages=1
    )

    result = await scraper.scrape_website(request)
    print(f"Data saved to: {result}")
    print()


async def example_s3_with_explicit_credentials():
    """Example: AWS S3 storage with explicit credentials"""
    print("=" * 60)
    print("Example 3: AWS S3 Storage with Explicit Credentials")
    print("=" * 60)

    # S3 storage with explicit credentials
    config = StorageConfig(
        storage_type="s3",
        bucket_name="my-scraper-bucket",
        aws_access_key_id="YOUR_ACCESS_KEY_ID",
        aws_secret_access_key="YOUR_SECRET_ACCESS_KEY",
        aws_region="us-west-2",
        partition_by_date=True,
        enable_fallback=False  # Raise error if S3 fails
    )

    scraper = WebsiteScraper(storage_config=config)

    request = ScrapingRequest(
        url="https://example.com",
        website_id="example",
        max_pages=1
    )

    result = await scraper.scrape_website(request)
    print(f"Data saved to: {result}")
    print()


async def example_gcs_with_credentials_file():
    """Example: Google Cloud Storage with credentials file"""
    print("=" * 60)
    print("Example 4: Google Cloud Storage with Credentials File")
    print("=" * 60)

    # GCS storage with credentials from JSON file
    config = StorageConfig(
        storage_type="gcs",
        bucket_name="my-scraper-bucket",
        gcs_credentials_file="gcp_credentials.json",
        partition_by_date=True,
        enable_fallback=True
    )

    scraper = WebsiteScraper(storage_config=config)

    request = ScrapingRequest(
        url="https://example.com",
        website_id="example",
        max_pages=1
    )

    result = await scraper.scrape_website(request)
    print(f"Data saved to: {result}")
    print()


async def example_gcs_with_env_credentials():
    """Example: Google Cloud Storage with environment credentials"""
    print("=" * 60)
    print("Example 5: Google Cloud Storage with Environment Credentials")
    print("=" * 60)
    print("Set GOOGLE_APPLICATION_CREDENTIALS environment variable")
    print()

    # GCS storage using default credentials from environment
    config = StorageConfig(
        storage_type="gcs",
        bucket_name="my-scraper-bucket",
        partition_by_date=True,
        enable_fallback=True
    )

    scraper = WebsiteScraper(storage_config=config)

    request = ScrapingRequest(
        url="https://example.com",
        website_id="example",
        max_pages=1
    )

    result = await scraper.scrape_website(request)
    print(f"Data saved to: {result}")
    print()


async def example_s3_without_fallback():
    """Example: S3 storage without fallback (strict mode)"""
    print("=" * 60)
    print("Example 6: S3 Storage Without Fallback (Strict Mode)")
    print("=" * 60)

    # If S3 fails, raise an error instead of falling back to local
    config = StorageConfig(
        storage_type="s3",
        bucket_name="my-scraper-bucket",
        aws_credentials_file="aws_credentials.json",
        enable_fallback=False  # Strict mode - fail if cloud storage fails
    )

    scraper = WebsiteScraper(storage_config=config)

    request = ScrapingRequest(
        url="https://example.com",
        website_id="example",
        max_pages=1
    )

    try:
        result = await scraper.scrape_website(request)
        print(f"Data saved to: {result}")
    except Exception as e:
        print(f"Error: {e}")
        print("Note: enable_fallback=False means errors are not caught")
    print()


async def example_scrape_multiple_with_s3():
    """Example: Scrape multiple URLs and save to S3"""
    print("=" * 60)
    print("Example 7: Scrape Multiple URLs to S3")
    print("=" * 60)

    config = StorageConfig(
        storage_type="s3",
        bucket_name="my-scraper-bucket",
        aws_credentials_file="aws_credentials.json",
        partition_by_date=True,
        enable_fallback=True
    )

    scraper = WebsiteScraper(
        storage_config=config,
        max_concurrent=3,  # Scrape 3 sites concurrently
        requests_per_second=2.0  # Rate limit: 2 requests/second
    )

    urls = [
        "https://example.com",
        "https://example.org",
        "https://example.net"
    ]

    results = await scraper.scrape_multiple_urls(urls)

    for url, result_path in results.items():
        print(f"{url} -> {result_path}")
    print()


async def example_custom_path_structure():
    """Example: Custom path structure without date partitioning"""
    print("=" * 60)
    print("Example 8: Custom Path Structure (No Date Partitioning)")
    print("=" * 60)

    # All files in flat structure: data/raw/website_id_timestamp.parquet
    config = StorageConfig(
        storage_type="local",
        output_dir="data/raw",
        partition_by_date=False  # No date-based subdirectories
    )

    scraper = WebsiteScraper(storage_config=config)

    request = ScrapingRequest(
        url="https://example.com",
        website_id="example",
        max_pages=1
    )

    result = await scraper.scrape_website(request)
    print(f"Data saved to: {result}")
    print()


# Main function to run all examples
async def main():
    """Run all examples"""
    print("\n" + "=" * 60)
    print("Storage Backend Examples")
    print("=" * 60)
    print()

    # Run examples (comment out cloud examples if you don't have credentials)
    await example_local_storage()

    # Uncomment these to test cloud storage (requires credentials)
    # await example_s3_with_credentials_file()
    # await example_s3_with_explicit_credentials()
    # await example_gcs_with_credentials_file()
    # await example_gcs_with_env_credentials()
    # await example_s3_without_fallback()
    # await example_scrape_multiple_with_s3()

    await example_custom_path_structure()

    print("=" * 60)
    print("All examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
