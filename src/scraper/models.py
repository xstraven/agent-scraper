"""Data models for the website scraper."""
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, HttpUrl


class ScrapingStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"


class ProxyConfig(BaseModel):
    """Proxy configuration for scraping."""
    server: str
    username: Optional[str] = None
    password: Optional[str] = None


class BrowserConfig(BaseModel):
    """Browser configuration for Playwright."""
    headless: bool = True
    user_agent: Optional[str] = None
    viewport_width: int = 1920
    viewport_height: int = 1080
    timeout: int = 30000
    wait_for_load_state: str = "networkidle"
    block_resources: List[str] = Field(default_factory=lambda: ["image", "stylesheet", "font", "media"])


class ScrapingRequest(BaseModel):
    """Request model for scraping a website."""
    url: HttpUrl
    website_id: Optional[str] = None
    max_pages: int = 1
    follow_links: bool = False
    custom_selectors: Optional[Dict[str, str]] = None
    extract_metadata: bool = True
    browser_config: Optional[BrowserConfig] = None
    proxy_config: Optional[ProxyConfig] = None


class ScrapedContent(BaseModel):
    """Scraped content from a single page."""
    url: str
    title: Optional[str] = None
    html: str
    text: str
    text_clean: Optional[str] = None  # Trafilatura-extracted clean text
    text_markdown: Optional[str] = None  # Markdown-formatted content
    metadata: Dict[str, Any] = Field(default_factory=dict)
    links: List[str] = Field(default_factory=list)
    images: List[str] = Field(default_factory=list)
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    load_time: float = 0.0
    status_code: Optional[int] = None
    error: Optional[str] = None


class ScrapingResult(BaseModel):
    """Complete scraping result for a website."""
    website_id: Optional[str] = None
    original_url: str
    pages: List[ScrapedContent] = Field(default_factory=list)
    status: ScrapingStatus = ScrapingStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_pages: int = 0
    successful_pages: int = 0
    failed_pages: int = 0
    error: Optional[str] = None
    
    @property
    def duration(self) -> Optional[float]:
        """Calculate scraping duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class StorageConfig(BaseModel):
    """Configuration for storing scraped data."""
    output_dir: str = "output"
    bucket_name: Optional[str] = None
    use_cloud_storage: bool = False
    partition_by_date: bool = True
    compression: str = "gzip"