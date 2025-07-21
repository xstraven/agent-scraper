# Flexible Website Scraper

A modern, async website scraper built with Python, Playwright, and pandas. Designed for scalable web scraping with automatic parquet file output and optional cloud storage support.

## Features

- **Async/Await Architecture**: Built on modern Python async/await for high performance
- **Playwright Browser Automation**: Handles JavaScript-heavy sites and SPAs
- **Flexible Data Output**: Automatic parquet file generation with pandas
- **Cloud Storage Support**: Direct integration with Google Cloud Storage
- **Rate Limiting & Concurrency**: Built-in throttling and concurrent request management
- **Robust Error Handling**: Comprehensive retry logic and error recovery
- **Multiple Input Methods**: Single URLs, URL lists, or CSV/Excel files
- **Configurable Browser Options**: Headless mode, proxy support, custom user agents
- **Link Following**: Automatic same-domain link discovery and crawling

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd agent-scraper

# Install dependencies
pip install -e .

# Install Playwright browsers
playwright install chromium
```

## Quick Start

### Command Line Usage

```bash
# Scrape a single URL
scraper --url https://example.com

# Scrape multiple URLs
scraper --urls https://example.com https://another-site.com

# Scrape from CSV file
scraper --file urls.csv --url-column website_url

# Advanced options
scraper --url https://example.com \
        --max-pages 5 \
        --follow-links \
        --max-concurrent 3 \
        --rate-limit 1.5 \
        --output-dir ./output

# Cloud storage
scraper --url https://example.com \
        --bucket-name my-bucket \
        --credentials service-account.json
```

### Python API

```python
import asyncio
from scraper import WebsiteScraper, ScrapingRequest, BrowserConfig, StorageConfig

async def scrape_website():
    # Configure storage
    storage_config = StorageConfig(
        output_dir="./scraped_data",
        partition_by_date=True,
        compression="gzip"
    )
    
    # Configure browser
    browser_config = BrowserConfig(
        headless=True,
        timeout=30000,
        viewport_width=1920,
        viewport_height=1080
    )
    
    # Initialize scraper
    scraper = WebsiteScraper(
        max_concurrent=3,
        requests_per_second=1.0,
        storage_config=storage_config
    )
    
    # Create request
    request = ScrapingRequest(
        url="https://example.com",
        website_id="example_site",
        max_pages=5,
        follow_links=True,
        browser_config=browser_config
    )
    
    # Scrape
    result = await scraper.scrape_website(request)
    
    print(f"Status: {result.status}")
    print(f"Pages scraped: {result.total_pages}")
    print(f"Duration: {result.duration:.2f}s")

# Run the scraper
asyncio.run(scrape_website())
```

## Configuration Options

### Browser Configuration

```python
from scraper import BrowserConfig, ProxyConfig

browser_config = BrowserConfig(
    headless=True,                    # Run in headless mode
    user_agent="custom-user-agent",   # Custom user agent
    viewport_width=1920,              # Browser viewport width
    viewport_height=1080,             # Browser viewport height
    timeout=30000,                    # Page load timeout (ms)
    wait_for_load_state="networkidle", # Load state to wait for
    block_resources=["image", "stylesheet", "font", "media"]  # Block resource types
)

# Proxy configuration
proxy_config = ProxyConfig(
    server="proxy.example.com:8080",
    username="user",
    password="pass"
)
```

### Storage Configuration

```python
from scraper import StorageConfig

# Local storage
storage_config = StorageConfig(
    output_dir="./output",
    partition_by_date=True,
    compression="gzip"
)

# Cloud storage
storage_config = StorageConfig(
    bucket_name="my-gcs-bucket",
    use_cloud_storage=True,
    partition_by_date=True,
    compression="gzip"
)
```

## Output Format

The scraper outputs data in Apache Parquet format with the following schema:

- `website_id`: Unique identifier for the website
- `original_url`: The starting URL that was requested
- `page_url`: URL of the individual page
- `title`: Page title
- `html`: Raw HTML content
- `text`: Extracted text content
- `scraped_at`: Timestamp when page was scraped
- `load_time`: Time taken to load the page (seconds)
- `status_code`: HTTP status code
- `error`: Error message if scraping failed
- `link_count`: Number of links found on the page
- `image_count`: Number of images found on the page
- `content_length`: Length of HTML content
- `text_length`: Length of text content
- `metadata_*`: Additional metadata columns

## Advanced Usage

### Custom Selectors

```python
request = ScrapingRequest(
    url="https://example.com",
    custom_selectors={
        "main_title": "h1.title",
        "description": ".description p",
        "price": ".price-value"
    }
)
```

### Batch Processing

```python
# Process URLs from CSV file
result_path = await scraper.scrape_from_file(
    "urls.csv",
    url_column="website_url",
    batch_size=50
)
```

### Multiple URL Scraping

```python
urls = ["https://site1.com", "https://site2.com", "https://site3.com"]
results = await scraper.scrape_multiple_urls(urls)
```

## Error Handling

The scraper includes comprehensive error handling:

- Automatic retries with exponential backoff
- Timeout handling for slow-loading pages
- Network error recovery
- Browser crash recovery
- Invalid URL handling

## Performance Considerations

- **Concurrency**: Limit concurrent browsers to avoid overwhelming target sites
- **Rate Limiting**: Use appropriate delays between requests
- **Resource Blocking**: Block unnecessary resources (images, CSS) for faster loading
- **Batch Processing**: Process large URL lists in batches

## Cloud Storage Integration

### Google Cloud Storage Setup

1. Create a GCS bucket
2. Set up service account credentials
3. Configure the scraper:

```python
storage_config = StorageConfig(
    bucket_name="my-scraping-bucket",
    use_cloud_storage=True
)

scraper = WebsiteScraper(
    storage_config=storage_config,
    credentials_path="service-account.json"
)
```

## Examples

See the `examples/` directory for more usage examples:

- `basic_usage.py`: Basic scraping examples
- `advanced_config.py`: Advanced configuration options
- `batch_processing.py`: Large-scale batch processing

## Architecture

The scraper is built with a modular architecture:

- **`models.py`**: Pydantic data models for requests and responses
- **`browser.py`**: Playwright browser management and page scraping
- **`scraper.py`**: Main scraping orchestration and coordination  
- **`storage.py`**: Storage backends (local and cloud)

## Dependencies

- Python 3.12+
- Playwright
- Pandas
- PyArrow (for Parquet files)
- Pydantic (for data validation)
- aiohttp (for async HTTP)
- Google Cloud Storage (optional)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

MIT License - see LICENSE file for details.