"""Main scraper implementation with async support."""
import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Set, Dict, Any
from urllib.parse import urljoin, urlparse
from asyncio_throttle import Throttler

from .models import (
    ScrapingRequest, 
    ScrapingResult, 
    ScrapedContent, 
    ScrapingStatus,
    BrowserConfig,
    ProxyConfig,
    StorageConfig
)
from .browser import BrowserManager
from .storage import get_storage_backend

logger = logging.getLogger(__name__)


class WebsiteScraper:
    """Main scraper class for crawling websites."""
    
    def __init__(
        self,
        max_concurrent: int = 3,
        requests_per_second: float = 1.0,
        storage_config: Optional[StorageConfig] = None
    ):
        self.max_concurrent = max_concurrent
        self.throttler = Throttler(rate_limit=requests_per_second)
        self.storage_config = storage_config or StorageConfig()
        self.storage_backend = get_storage_backend(self.storage_config)
        
        # Semaphore for controlling concurrency
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
    async def scrape_single_url(
        self, 
        url: str, 
        browser_config: Optional[BrowserConfig] = None,
        proxy_config: Optional[ProxyConfig] = None,
        custom_selectors: Optional[Dict[str, str]] = None
    ) -> ScrapedContent:
        """Scrape a single URL."""
        async with self.semaphore:
            async with self.throttler:
                async with BrowserManager(browser_config, proxy_config) as browser:
                    return await browser.scrape_page(url, custom_selectors)
                    
    async def scrape_website(self, request: ScrapingRequest) -> ScrapingResult:
        """Scrape a complete website based on the request."""
        result = ScrapingResult(
            website_id=request.website_id,
            original_url=str(request.url),
            status=ScrapingStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        try:
            # Start with the main URL
            urls_to_scrape = [str(request.url)]
            scraped_urls: Set[str] = set()
            all_pages: List[ScrapedContent] = []
            
            # Get domain for link filtering
            domain = urlparse(str(request.url)).netloc
            
            async with BrowserManager(request.browser_config, request.proxy_config) as browser:
                for current_url in urls_to_scrape[:request.max_pages]:
                    if current_url in scraped_urls:
                        continue
                        
                    logger.info(f"Scraping: {current_url}")
                    
                    async with self.semaphore:
                        async with self.throttler:
                            page_content = await browser.scrape_page(
                                current_url, 
                                request.custom_selectors
                            )
                            
                    scraped_urls.add(current_url)
                    all_pages.append(page_content)
                    
                    # If following links and haven't reached max pages
                    if (request.follow_links and 
                        len(scraped_urls) < request.max_pages and 
                        page_content.html and 
                        not page_content.error):
                        
                        # Extract and filter links
                        page_links = browser.extract_links(page_content.html, current_url)
                        same_domain_links = [
                            link for link in page_links 
                            if urlparse(link).netloc == domain and link not in scraped_urls
                        ]
                        
                        # Add new links to scrape queue
                        for link in same_domain_links[:5]:  # Limit new links per page
                            if link not in urls_to_scrape:
                                urls_to_scrape.append(link)
                                
                        logger.info(f"Found {len(same_domain_links)} same-domain links")
                        
            # Update result
            result.pages = all_pages
            result.total_pages = len(all_pages)
            result.successful_pages = sum(1 for p in all_pages if not p.error)
            result.failed_pages = sum(1 for p in all_pages if p.error)
            result.status = ScrapingStatus.SUCCESS
            result.completed_at = datetime.utcnow()
            
            # Save to storage
            storage_path = await self.storage_backend.save_result(result)
            logger.info(f"Saved scraping result to: {storage_path}")
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Scraping failed for {request.url}: {error_msg}")
            
            result.status = ScrapingStatus.FAILED
            result.error = error_msg
            result.completed_at = datetime.utcnow()
            
        return result
        
    async def scrape_multiple_urls(
        self, 
        urls: List[str],
        browser_config: Optional[BrowserConfig] = None,
        proxy_config: Optional[ProxyConfig] = None,
        custom_selectors: Optional[Dict[str, str]] = None
    ) -> List[ScrapedContent]:
        """Scrape multiple URLs concurrently."""
        
        async def scrape_single(url: str) -> ScrapedContent:
            return await self.scrape_single_url(url, browser_config, proxy_config, custom_selectors)
        
        # Create tasks for all URLs
        tasks = [scrape_single(url) for url in urls]
        
        # Execute with progress logging
        results = []
        completed = 0
        
        for coro in asyncio.as_completed(tasks):
            try:
                result = await coro
                results.append(result)
                completed += 1
                
                if completed % 10 == 0:
                    logger.info(f"Completed {completed}/{len(urls)} URLs")
                    
            except Exception as e:
                logger.error(f"Failed to scrape URL: {e}")
                completed += 1
                
        logger.info(f"Finished scraping {len(results)} URLs")
        return results
        
    async def scrape_from_file(
        self,
        file_path: str,
        url_column: str = "url",
        id_column: Optional[str] = None,
        batch_size: int = 100,
        **scrape_kwargs
    ) -> str:
        """Scrape URLs from a CSV/Excel file."""
        import pandas as pd
        
        # Read the file
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Unsupported file format. Use CSV or Excel.")
            
        if url_column not in df.columns:
            raise ValueError(f"Column '{url_column}' not found in file")
            
        urls = df[url_column].dropna().tolist()
        logger.info(f"Found {len(urls)} URLs to scrape")
        
        # Process in batches
        all_results = []
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(urls) + batch_size - 1)//batch_size}")
            
            batch_results = await self.scrape_multiple_urls(batch_urls, **scrape_kwargs)
            all_results.extend(batch_results)
            
            # Optional: save intermediate results
            if len(all_results) >= batch_size * 2:
                await self._save_batch_results(all_results, f"batch_{i//batch_size}")
                all_results = []
                
        # Save final results
        if all_results:
            storage_path = await self._save_batch_results(all_results, "final")
            return storage_path
            
        return "completed"
        
    async def _save_batch_results(self, results: List[ScrapedContent], batch_name: str) -> str:
        """Save a batch of results."""
        # Create a mock scraping result for storage
        result = ScrapingResult(
            website_id=batch_name,
            original_url="batch_processing",
            pages=results,
            status=ScrapingStatus.SUCCESS,
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            total_pages=len(results),
            successful_pages=sum(1 for r in results if not r.error),
            failed_pages=sum(1 for r in results if r.error)
        )
        
        return await self.storage_backend.save_result(result)