# Testing the Website Scraper

This document describes how to test the website scraper functionality.

## Quick Test

Run the simple keyword test to verify the scraper works:

```bash
# Run simple test that scrapes httpbin.org and checks for keywords
uv run python simple_test.py
```

This test will:
- Scrape `https://httpbin.org/html`
- Look for keywords: "html", "body", "httpbin"
- Display results and verify content was scraped successfully

## Comprehensive Test

Run the full end-to-end test:

```bash
# Run comprehensive test with multiple scenarios
uv run python test_example.py
```

This test includes:
- Single URL scraping with keyword detection
- Multiple URL scraping
- Custom CSS selectors
- Parquet file output verification
- Storage validation

## Pytest Tests

Run the structured pytest tests:

```bash
# Install test dependencies
uv add --group test pytest pytest-asyncio

# Run all tests
uv run pytest tests/ -v

# Run specific test
uv run pytest tests/test_scraper.py::test_keyword_search_simple -v
```

## Test Scenarios Covered

### 1. Keyword Detection Test
- **URL**: `https://httpbin.org/html`
- **Keywords**: `["html", "body", "httpbin"]`
- **Verification**: Checks if scraped content contains expected keywords

### 2. Multiple URL Scraping
- **URLs**: `["https://httpbin.org/html", "https://httpbin.org/json"]`
- **Verification**: All URLs scraped successfully

### 3. Custom Selectors
- **Selectors**: `{"page_title": "title", "first_heading": "h1"}`
- **Verification**: Custom data extracted via CSS selectors

### 4. Error Handling
- **URL**: Invalid domain
- **Verification**: Graceful error handling without crashes

### 5. Data Storage
- **Format**: Parquet files with proper schema
- **Verification**: Files created and data readable

## Expected Results

A successful test should show:

```
🎉 TEST PASSED!
   Successfully scraped content and found expected keywords.
```

With output showing:
- Status code: 200
- Content length > 0
- Keywords found in scraped content
- No errors during scraping

## Test Data

The tests use these reliable endpoints:

- **httpbin.org/html**: Returns predictable HTML content
- **httpbin.org/json**: Returns JSON data
- **example.com**: Standard example domain

## Troubleshooting

### Common Issues

1. **Playwright not installed**:
   ```bash
   uv run playwright install chromium
   ```

2. **Dependencies missing**:
   ```bash
   uv sync
   ```

3. **Network issues**: 
   - Check internet connection
   - Test sites may be temporarily unavailable

### Debug Mode

Run tests with more verbose output:

```bash
# Enable debug logging
PYTHONPATH=src python -c "
import logging
logging.basicConfig(level=logging.DEBUG)
import asyncio
from tests.test_scraper import test_keyword_search_simple
asyncio.run(test_keyword_search_simple())
"
```

## Adding New Tests

To add new test cases:

1. Add test function to `tests/test_scraper.py`
2. Use `@pytest.mark.asyncio` decorator
3. Follow the pattern of existing tests
4. Include keyword validation
5. Test both success and failure scenarios

Example:
```python
@pytest.mark.asyncio
async def test_my_website():
    scraper = WebsiteScraper()
    content = await scraper.scrape_single_url("https://example.com")
    
    assert "expected_keyword" in content.text.lower()
    assert content.error is None
    assert content.status_code == 200
```