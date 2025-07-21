"""Flexible website scraper built on async Python and Playwright."""
import asyncio
import logging
from typing import List, Dict, Any, Optional

from .scraper import WebsiteScraper
from .models import (
    ScrapingRequest,
    ScrapingResult,
    ScrapedContent,
    BrowserConfig,
    ProxyConfig,
    StorageConfig,
    ScrapingStatus
)
from .browser import BrowserManager
from .storage import get_storage_backend

__version__ = "0.1.0"
__all__ = [
    "WebsiteScraper",
    "ScrapingRequest",
    "ScrapingResult", 
    "ScrapedContent",
    "BrowserConfig",
    "ProxyConfig",
    "StorageConfig",
    "ScrapingStatus",
    "BrowserManager",
    "get_storage_backend",
    "main"
]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Main entry point for CLI usage."""
    import argparse
    import sys
    from pathlib import Path
    
    parser = argparse.ArgumentParser(description="Flexible website scraper")
    parser.add_argument("--url", help="Single URL to scrape")
    parser.add_argument("--urls", nargs="+", help="Multiple URLs to scrape")
    parser.add_argument("--file", help="CSV/Excel file with URLs")
    parser.add_argument("--url-column", default="url", help="Column name for URLs in file")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--max-pages", type=int, default=1, help="Max pages per website")
    parser.add_argument("--follow-links", action="store_true", help="Follow links on the same domain")
    parser.add_argument("--max-concurrent", type=int, default=3, help="Max concurrent browsers")
    parser.add_argument("--rate-limit", type=float, default=1.0, help="Requests per second")
    parser.add_argument("--headless", action="store_true", default=True, help="Run browser in headless mode")
    parser.add_argument("--timeout", type=int, default=30000, help="Page load timeout in milliseconds")
    parser.add_argument("--bucket-name", help="Google Cloud Storage bucket name")
    parser.add_argument("--credentials", help="Path to GCS credentials file")
    
    args = parser.parse_args()
    
    if not any([args.url, args.urls, args.file]):
        print("Error: Must provide --url, --urls, or --file")
        sys.exit(1)
    
    async def run_scraper():
        # Configure storage
        storage_config = StorageConfig(
            output_dir=args.output_dir,
            bucket_name=args.bucket_name,
            use_cloud_storage=bool(args.bucket_name)
        )
        
        # Configure browser
        browser_config = BrowserConfig(
            headless=args.headless,
            timeout=args.timeout
        )
        
        # Initialize scraper
        scraper = WebsiteScraper(
            max_concurrent=args.max_concurrent,
            requests_per_second=args.rate_limit,
            storage_config=storage_config,
            credentials_path=args.credentials
        )
        
        if args.file:
            # Scrape from file
            result_path = await scraper.scrape_from_file(
                args.file,
                url_column=args.url_column,
                browser_config=browser_config
            )
            print(f"Scraping completed. Results saved to: {result_path}")
            
        elif args.urls:
            # Scrape multiple URLs
            results = await scraper.scrape_multiple_urls(
                args.urls,
                browser_config=browser_config
            )
            print(f"Scraped {len(results)} URLs")
            
        elif args.url:
            # Scrape single website
            request = ScrapingRequest(
                url=args.url,
                max_pages=args.max_pages,
                follow_links=args.follow_links,
                browser_config=browser_config
            )
            
            result = await scraper.scrape_website(request)
            print(f"Scraping completed with status: {result.status}")
            print(f"Total pages: {result.total_pages}")
            print(f"Successful: {result.successful_pages}")
            print(f"Failed: {result.failed_pages}")
            if result.duration:
                print(f"Duration: {result.duration:.2f} seconds")
    
    # Run the async function
    try:
        asyncio.run(run_scraper())
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
