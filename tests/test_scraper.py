"""Simple tests for the website scraper."""

import asyncio
import pytest
from unittest.mock import patch, MagicMock

from scraper import (
    WebsiteScraper,
    ScrapingRequest,
    BrowserConfig,
    StorageConfig,
    ScrapingStatus,
)


class TestWebsiteScraper:
    """Test cases for the website scraper."""

    @pytest.fixture
    def scraper(self):
        """Create a test scraper instance."""
        storage_config = StorageConfig(
            output_dir="./test_output", use_cloud_storage=False
        )
        return WebsiteScraper(
            max_concurrent=1,
            requests_per_second=2.0,
            storage_config=storage_config,
        )

    @pytest.fixture
    def browser_config(self):
        """Create test browser configuration."""
        return BrowserConfig(
            headless=True, timeout=15000, wait_for_load_state="domcontentloaded"
        )

    @pytest.mark.asyncio
    async def test_scrape_httpbin_html(self, scraper, browser_config):
        """Test scraping httpbin.org/html and check for known content."""
        # Create scraping request for httpbin HTML endpoint
        request = ScrapingRequest(
            url="https://httpbin.org/html",
            website_id="httpbin_test",
            max_pages=1,
            browser_config=browser_config,
        )

        # Scrape the website
        result = await scraper.scrape_website(request)

        # Verify scraping was successful
        assert result.status == ScrapingStatus.SUCCESS
        assert result.total_pages == 1
        assert result.successful_pages == 1
        assert result.failed_pages == 0
        assert len(result.pages) == 1

        # Get the scraped page
        page = result.pages[0]

        # Verify page content
        assert page.url == "https://httpbin.org/html"
        assert page.title is not None
        assert page.html != ""
        assert page.text != ""
        assert page.error is None
        assert page.status_code == 200

        # Check for known keywords in the HTML content
        html_content = page.html.lower()
        assert "html" in html_content
        assert "httpbin" in html_content

        # Check for known keywords in the text content
        text_content = page.text.lower()
        assert "httpbin" in text_content or "html" in text_content

        print(f"✓ Successfully scraped {page.url}")
        print(f"✓ Title: {page.title}")
        print(f"✓ Text length: {len(page.text)} characters")
        print(f"✓ HTML length: {len(page.html)} characters")

    @pytest.mark.asyncio
    async def test_scrape_example_com(self, scraper, browser_config):
        """Test scraping example.com and check for known content."""
        request = ScrapingRequest(
            url="https://example.com",
            website_id="example_test",
            max_pages=1,
            browser_config=browser_config,
        )

        result = await scraper.scrape_website(request)

        # Verify scraping was successful
        assert result.status == ScrapingStatus.SUCCESS
        assert result.total_pages == 1
        assert len(result.pages) == 1

        page = result.pages[0]

        # Verify basic page properties
        assert page.url == "https://example.com/"
        assert page.error is None
        assert page.status_code == 200

        # Check for known content on example.com
        text_content = page.text.lower()
        html_content = page.html.lower()

        # Example.com should contain these keywords
        expected_keywords = ["example", "domain", "illustrative"]

        found_keywords = []
        for keyword in expected_keywords:
            if keyword in text_content or keyword in html_content:
                found_keywords.append(keyword)

        # At least one expected keyword should be found
        assert (
            len(found_keywords) > 0
        ), f"None of the expected keywords {expected_keywords} found in content"

        print(f"✓ Successfully scraped {page.url}")
        print(f"✓ Found keywords: {found_keywords}")
        print(f"✓ Page title: {page.title}")

    @pytest.mark.asyncio
    async def test_multiple_urls_with_keywords(self, scraper, browser_config):
        """Test scraping multiple URLs and check for specific keywords."""
        urls = ["https://httpbin.org/html", "https://httpbin.org/json"]

        results = await scraper.scrape_multiple_urls(
            urls, browser_config=browser_config
        )

        # Should have results for both URLs
        assert len(results) == 2

        # Check each result
        for result in results:
            assert (
                result.error is None or result.error == ""
            ), f"Error scraping {result.url}: {result.error}"
            assert (
                len(result.text) > 0
            ), f"No text content found for {result.url}"

            # Check for httpbin keyword in all results
            content = (result.text + result.html).lower()
            assert (
                "httpbin" in content
            ), f"'httpbin' keyword not found in {result.url}"

        print(f"✓ Successfully scraped {len(results)} URLs")
        for result in results:
            print(f"  - {result.url}: {len(result.text)} chars")

    @pytest.mark.asyncio
    async def test_custom_selectors(self, scraper, browser_config):
        """Test scraping with custom CSS selectors."""
        request = ScrapingRequest(
            url="https://httpbin.org/html",
            website_id="selector_test",
            max_pages=1,
            browser_config=browser_config,
            custom_selectors={"page_title": "title", "first_heading": "h1"},
        )

        result = await scraper.scrape_website(request)

        assert result.status == ScrapingStatus.SUCCESS
        assert len(result.pages) == 1

        page = result.pages[0]

        # Check if custom selectors extracted data
        assert "page_title" in page.metadata or "first_heading" in page.metadata

        print(f"✓ Custom selectors extracted: {list(page.metadata.keys())}")

    @pytest.mark.asyncio
    async def test_invalid_url_handling(self, scraper, browser_config):
        """Test that invalid URLs are handled gracefully."""
        request = ScrapingRequest(
            url="https://this-domain-should-not-exist-12345.com",
            website_id="invalid_test",
            max_pages=1,
            browser_config=browser_config,
        )

        result = await scraper.scrape_website(request)

        # Should complete but with errors
        assert result.total_pages == 1
        assert result.failed_pages == 1
        assert len(result.pages) == 1

        page = result.pages[0]
        assert page.error is not None
        assert len(page.html) == 0
        assert len(page.text) == 0

        print(f"✓ Invalid URL handled gracefully: {page.error}")


@pytest.mark.asyncio
async def test_keyword_search_simple():
    """Simple test function that can be run independently."""
    storage_config = StorageConfig(output_dir="./test_output")
    scraper = WebsiteScraper(storage_config=storage_config)

    browser_config = BrowserConfig(headless=True, timeout=15000)

    # Test with a reliable URL
    url = "https://httpbin.org/html"
    content = await scraper.scrape_single_url(url, browser_config)

    # Check for known keywords
    text_lower = content.text.lower()
    html_lower = content.html.lower()

    # HTTPBin should contain these
    expected_keywords = ["httpbin", "html"]
    found_keywords = []

    for keyword in expected_keywords:
        if keyword in text_lower or keyword in html_lower:
            found_keywords.append(keyword)

    assert (
        len(found_keywords) > 0
    ), f"Expected keywords {expected_keywords} not found"

    print(f"✅ SUCCESS: Found keywords {found_keywords} in {url}")
    print(f"   - Page title: {content.title}")
    print(f"   - Content length: {len(content.text)} characters")
    print(f"   - Status code: {content.status_code}")

    return content


if __name__ == "__main__":
    # Run the simple test directly
    asyncio.run(test_keyword_search_simple())
