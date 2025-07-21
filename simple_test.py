#!/usr/bin/env python3
"""
Simple test that scrapes a website and checks for keywords.
This demonstrates the core functionality of the scraper.
"""
import asyncio
import sys
from pathlib import Path

# Add src to path so we can import scraper
sys.path.insert(0, str(Path(__file__).parent / "src"))

from scraper import WebsiteScraper, BrowserConfig, StorageConfig

async def test_keyword_scraping():
    """Test scraping a website and checking for specific keywords."""
    
    print("🔍 Testing Keyword Scraping with Website Scraper")
    print("=" * 50)
    
    # Configure the scraper
    storage_config = StorageConfig(output_dir="./test_output")
    browser_config = BrowserConfig(
        headless=True,
        timeout=20000
    )
    
    scraper = WebsiteScraper(
        max_concurrent=1,
        requests_per_second=1.0,
        storage_config=storage_config
    )
    
    # Test URL and expected keywords
    test_url = "https://httpbin.org/html"
    expected_keywords = ["html", "body", "httpbin"]
    
    print(f"🌐 Scraping URL: {test_url}")
    print(f"🎯 Looking for keywords: {expected_keywords}")
    print()
    
    # Scrape the page
    result = await scraper.scrape_single_url(test_url, browser_config)
    
    # Display basic results
    print("📊 Scraping Results:")
    print(f"   Status Code: {result.status_code}")
    print(f"   Page Title: {result.title or 'N/A'}")
    print(f"   Load Time: {result.load_time:.2f} seconds")
    print(f"   HTML Length: {len(result.html):,} characters")
    print(f"   Text Length: {len(result.text):,} characters")
    print(f"   Links Found: {len(result.links)}")
    print()
    
    # Check for keywords
    print("🔍 Keyword Analysis:")
    content = (result.html + " " + result.text).lower()
    
    found_keywords = []
    missing_keywords = []
    
    for keyword in expected_keywords:
        if keyword.lower() in content:
            found_keywords.append(keyword)
            print(f"   ✅ Found: '{keyword}'")
        else:
            missing_keywords.append(keyword)
            print(f"   ❌ Missing: '{keyword}'")
    
    print()
    print(f"📈 Results Summary:")
    print(f"   Found: {len(found_keywords)}/{len(expected_keywords)} keywords")
    print(f"   Keywords found: {found_keywords}")
    
    if missing_keywords:
        print(f"   Keywords missing: {missing_keywords}")
    
    # Test passed if we found at least one keyword and no errors
    success = len(found_keywords) > 0 and result.error is None
    
    if success:
        print(f"\n🎉 TEST PASSED!")
        print(f"   Successfully scraped content and found expected keywords.")
    else:
        print(f"\n❌ TEST FAILED!")
        if result.error:
            print(f"   Error: {result.error}")
        if len(found_keywords) == 0:
            print(f"   No expected keywords found in content.")
    
    return success

async def main():
    """Run the keyword scraping test."""
    try:
        success = await test_keyword_scraping()
        if not success:
            sys.exit(1)
    except Exception as e:
        print(f"\n💥 Error running test: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())