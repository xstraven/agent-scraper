"""Example usage of the website scraper."""
import asyncio
from scraper import (
    WebsiteScraper,
    ScrapingRequest,
    BrowserConfig,
    StorageConfig
)

async def basic_example():
    """Basic scraping example."""
    # Configure storage (local by default)
    storage_config = StorageConfig(
        output_dir="./scraped_data",
        partition_by_date=True
    )
    
    # Configure browser
    browser_config = BrowserConfig(
        headless=True,
        timeout=30000
    )
    
    # Initialize scraper
    scraper = WebsiteScraper(
        max_concurrent=2,
        requests_per_second=1.0,
        storage_config=storage_config
    )
    
    # Create scraping request
    request = ScrapingRequest(
        url="https://example.com",
        website_id="example_site",
        max_pages=3,
        follow_links=True,
        browser_config=browser_config
    )
    
    # Scrape the website
    result = await scraper.scrape_website(request)
    
    print(f"Scraping completed!")
    print(f"Status: {result.status}")
    print(f"Total pages: {result.total_pages}")
    print(f"Successful pages: {result.successful_pages}")
    print(f"Duration: {result.duration:.2f} seconds")
    

async def multi_url_example():
    """Example of scraping multiple URLs."""
    scraper = WebsiteScraper(max_concurrent=3)
    
    urls = [
        "https://httpbin.org/html",
        "https://example.com",
        "https://httpbin.org/json"
    ]
    
    results = await scraper.scrape_multiple_urls(urls)
    
    for result in results:
        print(f"URL: {result.url}")
        print(f"Title: {result.title}")
        print(f"Text length: {len(result.text)}")
        print(f"Error: {result.error}")
        print("---")


async def cloud_storage_example():
    """Example with Google Cloud Storage."""
    storage_config = StorageConfig(
        bucket_name="your-bucket-name",
        use_cloud_storage=True
    )
    
    scraper = WebsiteScraper(
        storage_config=storage_config,
        credentials_path="path/to/service-account.json"  # Optional
    )
    
    request = ScrapingRequest(
        url="https://example.com",
        website_id="cloud_example"
    )
    
    result = await scraper.scrape_website(request)
    print(f"Data saved to Google Cloud Storage: {result.status}")


if __name__ == "__main__":
    # Run basic example
    asyncio.run(basic_example())
    
    # Run multi-URL example
    # asyncio.run(multi_url_example())
    
    # Run cloud storage example (requires GCS setup)
    # asyncio.run(cloud_storage_example())