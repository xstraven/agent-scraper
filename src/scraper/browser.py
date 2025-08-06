"""Browser management for async web scraping."""
import asyncio
import logging
import random
import time
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Error as PlaywrightError
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
import trafilatura

from .models import BrowserConfig, ProxyConfig, ScrapedContent, ScrapingStatus

logger = logging.getLogger(__name__)


class BrowserManager:
    """Manages browser instances and page operations."""
    
    DEFAULT_USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0"
    ]
    
    def __init__(self, config: Optional[BrowserConfig] = None, proxy_config: Optional[ProxyConfig] = None):
        self.config = config or BrowserConfig()
        self.proxy_config = proxy_config
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        
    async def __aenter__(self):
        await self.start()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def start(self):
        """Start the browser instance."""
        try:
            self.playwright = await async_playwright().start()
            
            # Browser launch options
            launch_options = {
                "headless": self.config.headless,
                "args": [
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-web-security",
                    "--disable-features=VizDisplayCompositor"
                ]
            }
            
            # Add proxy if configured
            if self.proxy_config:
                launch_options["proxy"] = {
                    "server": self.proxy_config.server,
                    "username": self.proxy_config.username,
                    "password": self.proxy_config.password
                }
            
            self.browser = await self.playwright.chromium.launch(**launch_options)
            
            # Create context with configuration
            context_options = {
                "viewport": {
                    "width": self.config.viewport_width,
                    "height": self.config.viewport_height
                },
                "user_agent": self.config.user_agent or random.choice(self.DEFAULT_USER_AGENTS),
                "extra_http_headers": {
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1"
                }
            }
            
            self.context = await self.browser.new_context(**context_options)
            
            # Set up request interception if resource blocking is enabled
            if self.config.block_resources:
                await self.context.route("**/*", self._intercept_request)
                
        except Exception as e:
            logger.error(f"Failed to start browser: {e}")
            await self.close()
            raise
            
    async def _intercept_request(self, route, request):
        """Intercept and potentially block requests."""
        try:
            if request.resource_type in self.config.block_resources:
                await route.abort()
            else:
                await route.continue_()
        except PlaywrightError as e:
            logger.warning(f"Route interception failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in request interception: {e}")
            
    async def close(self):
        """Close the browser and all resources."""
        try:
            if self.context:
                await self.context.close()
                self.context = None
            if self.browser:
                await self.browser.close()
                self.browser = None
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
        except Exception as e:
            logger.error(f"Error closing browser: {e}")
            
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def scrape_page(self, url: str, custom_selectors: Optional[Dict[str, str]] = None) -> ScrapedContent:
        """Scrape a single page."""
        if not self.context:
            raise RuntimeError("Browser not started. Use async context manager or call start() first.")
            
        page: Optional[Page] = None
        start_time = time.time()
        
        try:
            page = await self.context.new_page()
            
            # Navigate to the page
            response = await page.goto(
                str(url), 
                wait_until=self.config.wait_for_load_state,
                timeout=self.config.timeout
            )
            
            # Wait for any dynamic content
            await asyncio.sleep(1)
            
            # Extract content
            html = await page.content()
            title = await page.title()
            
            # Extract text content
            text = await page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll('script, style, noscript');
                    scripts.forEach(script => script.remove());
                    return document.body ? document.body.innerText : '';
                }
            """)
            
            # Extract clean text using trafilatura
            text_clean = None
            text_markdown = None
            try:
                text_clean = trafilatura.extract(html, output_format='txt')
                text_markdown = trafilatura.extract(html, output_format='markdown')
            except Exception as e:
                logger.warning(f"Trafilatura extraction failed for {url}: {e}")
            
            # Extract links
            links = await page.evaluate("""
                () => Array.from(document.querySelectorAll('a[href]'), a => a.href)
            """)
            
            # Extract images
            images = await page.evaluate("""
                () => Array.from(document.querySelectorAll('img[src]'), img => img.src)
            """)
            
            # Extract custom data if selectors provided
            metadata = {}
            if custom_selectors:
                for key, selector in custom_selectors.items():
                    try:
                        element = await page.query_selector(selector)
                        if element:
                            metadata[key] = await element.inner_text()
                    except Exception as e:
                        logger.warning(f"Failed to extract {key} with selector {selector}: {e}")
            
            # Extract additional metadata
            metadata.update({
                "page_url": str(url),
                "final_url": page.url,
                "status_code": response.status if response else None,
                "content_type": response.headers.get("content-type") if response else None,
                "content_length": len(html),
                "text_length": len(text),
                "text_clean_length": len(text_clean) if text_clean else 0,
                "text_markdown_length": len(text_markdown) if text_markdown else 0,
                "link_count": len(links),
                "image_count": len(images)
            })
            
            load_time = time.time() - start_time
            
            return ScrapedContent(
                url=str(url),
                title=title,
                html=html,
                text=text,
                text_clean=text_clean,
                text_markdown=text_markdown,
                metadata=metadata,
                links=links,
                images=images,
                load_time=load_time,
                status_code=response.status if response else None
            )
            
        except asyncio.TimeoutError:
            error_msg = f"Timeout scraping {url}"
            logger.error(error_msg)
            return ScrapedContent(
                url=str(url),
                html="",
                text="",
                text_clean=None,
                text_markdown=None,
                load_time=time.time() - start_time,
                error=error_msg
            )
            
        except Exception as e:
            error_msg = f"Error scraping {url}: {str(e)}"
            logger.error(error_msg)
            return ScrapedContent(
                url=str(url),
                html="",
                text="",
                text_clean=None,
                text_markdown=None,
                load_time=time.time() - start_time,
                error=error_msg
            )
            
        finally:
            if page:
                try:
                    await page.close()
                except Exception as e:
                    logger.warning(f"Error closing page: {e}")
                    
    def extract_links(self, html: str, base_url: str) -> List[str]:
        """Extract and normalize links from HTML."""
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Convert relative URLs to absolute
            full_url = urljoin(base_url, href)
            
            # Basic URL validation
            parsed = urlparse(full_url)
            if parsed.scheme in ['http', 'https'] and parsed.netloc:
                links.append(full_url)
                
        return list(set(links))  # Remove duplicates