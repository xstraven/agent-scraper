# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a dual-purpose web scraping system with two distinct components:

1. **General Web Scraper** (`/src/scraper/`): Modern async scraper using Playwright for JavaScript-heavy sites, with multi-format text extraction (raw, clean text via Trafilatura, Markdown) and Parquet output
2. **Municipal Protocol Scraper** (`/src/municipal_scraper/`): Specialized scraper for German RIS (Ratsinformationssystem) systems that discovers and extracts council meeting protocols from German municipalities

Both use modern Python async/await architecture with strong type safety via Pydantic.

## Development Commands

### Setup
```bash
# Install dependencies
uv sync

# Install Playwright browsers (required for scraping)
uv run playwright install chromium
```

### Running the Scrapers

**General web scraper (has CLI):**
```bash
# Single URL
uv run scraper --url https://example.com

# Multiple URLs with options
uv run scraper --urls https://site1.com https://site2.com --max-pages 5 --follow-links

# From CSV file
uv run scraper --file urls.csv --url-column website_url
```

**Municipal scraper (Python API only - no CLI yet):**
```bash
# Run the real-world test example
uv run python src/municipal_scraper/test_amt_mittelholstein.py
```

### Testing

```bash
# Quick validation test
uv run python simple_test.py

# Full end-to-end test
uv run python test_example.py

# Run pytest suite
uv run pytest tests/ -v

# Run specific test
uv run pytest tests/test_scraper.py::test_keyword_search_simple -v

# Municipal scraper integration test
uv run python src/municipal_scraper/test_amt_mittelholstein.py
```

## Architecture

### General Scraper (`/src/scraper/`)

**Key Components:**
- **models.py**: Pydantic data models for type-safe requests/responses
  - `ScrapingRequest`: Input configuration (URL, browser settings, selectors)
  - `ScrapedContent`: Single page result with multiple text formats
  - `ScrapingResult`: Complete scraping session with metadata

- **browser.py**: Playwright browser lifecycle and page scraping
  - `BrowserManager`: Context manager for browser instances
  - Multi-format text extraction: raw text, Trafilatura clean text, Trafilatura Markdown
  - Request interception for resource blocking
  - User agent rotation (5 default agents)
  - Retry logic with tenacity (3 attempts, exponential backoff)

- **scraper.py**: Main orchestration
  - `WebsiteScraper`: Async coordinator with semaphore-based concurrency control
  - Rate limiting via asyncio-throttle
  - Link crawling with same-domain filtering
  - Batch processing for CSV/Excel files

- **storage.py**: Storage backend abstraction
  - `LocalStorageBackend`: Filesystem with date partitioning
  - `CloudStorageBackend`: Google Cloud Storage integration
  - Parquet output with gzip compression (14+ columns per page)

**Data Flow:**
```
URL(s) → BrowserManager → Playwright → Content Extraction (HTML/Text/Clean/Markdown)
→ ScrapedContent → Rate Limiting → ScrapingResult → Storage (Parquet files)
```

### Municipal Scraper (`/src/municipal_scraper/`)

**Key Components:**
- **data_models.py**: German-specific Pydantic models
  - Enums: `GermanState`, `AdministrativeLevel`, `RISProvider`, `DocumentType`
  - Models: `Municipality`, `Meeting`, `Protocol`, `MeetingDocument`
  - Discovery and session tracking models

- **target_discovery.py**: RIS URL discovery
  - `TargetDiscovery`: Finds RIS systems for municipalities
  - Pattern-based URL generation (common RIS paths)
  - Main website analysis with aiohttp
  - RIS scoring algorithm (0.0-1.0 confidence)
  - Provider detection from HTML signatures
  - Supports: Regisafe, SD.NET, SessionNet, AllRIS, Kommune-Aktiv, Somacos

- **protocol_scraper.py**: Meeting/document extraction
  - `ProtocolScraper`: Extracts meetings and documents
  - Provider-specific discovery methods (each RIS has different HTML structure)
  - Document download with aiohttp
  - Meeting date parsing (German date formats)
  - Document type classification
  - **Note**: Protocol content extraction (line 477) is currently a placeholder

**Data Flow:**
```
Municipality → TargetDiscovery → RIS URL + Provider Detection
→ ProtocolScraper → Meeting Discovery → Document Links → Download
→ ScrapingSession (with metadata)
```

### Supported RIS Providers

The municipal scraper detects and handles 6 RIS providers, each with specific HTML patterns:
1. **Regisafe** (e.g., Amt Mittelholstein) - Full support
2. **SD.NET** - Falls back to generic (incomplete)
3. **SessionNet** - Full support
4. **AllRIS** - Full support
5. **Kommune-Aktiv** - Full support
6. **Somacos** - Full support

## Important Implementation Details

### Text Extraction Formats

The general scraper provides **three text formats** (browser.py:~180-250):
1. **Raw text** (`text`): JavaScript-based extraction from page
2. **Clean text** (`text_clean`): Trafilatura extraction, removes boilerplate
3. **Markdown** (`text_markdown`): Trafilatura Markdown format, preserves structure

Always populate all three when scraping. Trafilatura failures should be logged but not stop scraping.

### Concurrency & Rate Limiting

Both scrapers use async patterns:
- **Semaphore** (`asyncio.Semaphore`) for max concurrent browsers
- **Rate limiting** (`asyncio-throttle`) for requests per second
- Default: 3 concurrent, 1 request/second (configurable)

### Error Handling Pattern

All scrapers follow this pattern:
1. **Retry with backoff** (via tenacity decorator)
2. **Graceful degradation** (continue on non-critical failures)
3. **Error tracking** (store errors in data models, don't fail fast)
4. **Timeout handling** (explicit asyncio.TimeoutError catches)

### Storage Output

- **Format**: Apache Parquet (columnar, compressed)
- **Partitioning**: Optional date-based (`YYYY/MM/DD/`)
- **Naming**: `{website_id}_{timestamp}.parquet`
- **Schema**: 14+ base columns + dynamic `metadata_*` columns

### German Date Parsing

Municipal scraper handles various German date formats (protocol_scraper.py:~155-175):
- "Mittwoch, 15. Januar 2025"
- "15.01.2025"
- "2025-01-15"

Uses regex patterns with fallback logic.

## Known Limitations & TODOs

1. **Protocol content extraction incomplete** (protocol_scraper.py:477)
   - Placeholder returns empty string
   - Needs PDF/DOCX/HTML text extraction implementation

2. **No CLI for municipal scraper**
   - Only accessible via Python API
   - Consider adding `municipal_scraper/cli.py`

3. **SD.NET provider incomplete** (protocol_scraper.py:554)
   - Falls back to generic discovery
   - Needs specific HTML parsing implementation

4. **No incremental scraping**
   - Re-scrapes all content each time
   - No deduplication or checkpoint/resume

5. **Search-based discovery not implemented** (target_discovery.py:201)
   - Only pattern-based URL generation works
   - Commented out search engine integration

## Testing Strategy

The project uses **three test levels**:

1. **Unit tests** (`tests/test_scraper.py`): Pytest with async fixtures
2. **Integration tests** (`simple_test.py`, `test_example.py`): Real HTTP requests
3. **Real-world tests** (`municipal_scraper/test_amt_mittelholstein.py`): Actual German municipality

Tests validate:
- Keyword detection in scraped content
- Multi-format text extraction (raw/clean/markdown)
- Storage output (Parquet file creation)
- Error handling (invalid URLs, timeouts)
- Custom CSS selectors
- Municipal RIS discovery and scraping

**Key test sites:**
- `httpbin.org/html` - Predictable HTML
- `example.com` - Standard test domain
- Amt Mittelholstein - Real German municipality

## Configuration

All scrapers use Pydantic models for configuration:

**Browser:**
```python
BrowserConfig(
    headless=True,
    timeout=30000,  # milliseconds
    viewport_width=1920,
    viewport_height=1080,
    block_resources=["image", "stylesheet", "font", "media"]
)
```

**Storage:**
```python
StorageConfig(
    output_dir="./output",
    partition_by_date=True,
    compression="gzip",
    # For GCS:
    bucket_name="my-bucket",
    use_cloud_storage=True
)
```

**Scraping:**
```python
ScrapingRequest(
    url="https://example.com",
    website_id="unique_id",
    max_pages=5,
    follow_links=True,
    custom_selectors={"title": "h1.main"}
)
```

## Adding New Features

When extending the scrapers:

1. **Add Pydantic models first** (in `models.py` or `data_models.py`)
2. **Use async/await** throughout (no blocking calls)
3. **Handle errors gracefully** (log warnings, don't fail fast)
4. **Add retry logic** for network operations
5. **Update storage schema** if adding new data fields
6. **Write tests** using pytest-asyncio
7. **Document in docstrings** (especially complex logic)

For municipal scraper provider support:
1. Add HTML signature to `_detect_provider_from_html()` (target_discovery.py:~250)
2. Implement `_discover_meetings_{provider}()` method (protocol_scraper.py)
3. Test with real municipality using that provider
