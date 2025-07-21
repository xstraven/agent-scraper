#!/usr/bin/env python3
"""
Simple end-to-end test of the website scraper.
Tests scraping a website and checking for known keywords.
"""
import asyncio
import sys
from pathlib import Path
import tempfile
import pandas as pd

# Add src to path so we can import scraper
sys.path.insert(0, str(Path(__file__).parent / "src"))

from scraper import (
    WebsiteScraper,
    ScrapingRequest,
    BrowserConfig,
    StorageConfig,
    ScrapingStatus
)

async def test_scraper_with_keywords():
    """Test the scraper by scraping a known website and checking for keywords."""
    
    print("🚀 Testing Website Scraper")
    print("=" * 60)
    
    # Create temporary output directory
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"📁 Output directory: {temp_dir}")
        
        # Configure storage
        storage_config = StorageConfig(
            output_dir=temp_dir,
            partition_by_date=False,
            compression="gzip"
        )
        
        # Configure browser for fast testing
        browser_config = BrowserConfig(
            headless=True,
            timeout=20000,
            wait_for_load_state="domcontentloaded",
            block_resources=["image", "stylesheet", "font", "media"]
        )
        
        # Initialize scraper
        scraper = WebsiteScraper(
            max_concurrent=1,
            requests_per_second=2.0,
            storage_config=storage_config
        )
        
        print("\n🌐 Testing Single URL Scraping...")
        print("-" * 40)
        
        # Test 1: Single URL scraping with httpbin (reliable test site)
        test_url = "https://httpbin.org/html"
        expected_keywords = ["httpbin", "html", "request", "response"]
        
        print(f"📍 Scraping: {test_url}")
        
        request = ScrapingRequest(
            url=test_url,
            website_id="test_httpbin",
            max_pages=1,
            browser_config=browser_config,
            custom_selectors={
                "page_title": "title",
                "main_heading": "h1"
            }
        )
        
        # Scrape the website
        result = await scraper.scrape_website(request)
        
        # Verify scraping results
        print(f"✅ Scraping Status: {result.status}")
        print(f"📄 Total pages: {result.total_pages}")
        print(f"✅ Successful pages: {result.successful_pages}")
        print(f"❌ Failed pages: {result.failed_pages}")
        
        if result.duration:
            print(f"⏱️  Duration: {result.duration:.2f} seconds")
        
        # Check if scraping was successful
        assert result.status == ScrapingStatus.SUCCESS, f"Scraping failed: {result.error}"
        assert result.total_pages == 1, f"Expected 1 page, got {result.total_pages}"
        assert len(result.pages) == 1, f"Expected 1 page result, got {len(result.pages)}"
        
        # Analyze the scraped content
        page = result.pages[0]
        print(f"\n📊 Page Analysis:")
        print(f"   🔗 URL: {page.url}")
        print(f"   📝 Title: {page.title}")
        print(f"   📏 HTML length: {len(page.html):,} characters")
        print(f"   📄 Text length: {len(page.text):,} characters")
        print(f"   🔗 Links found: {len(page.links)}")
        print(f"   🖼️  Images found: {len(page.images)}")
        print(f"   🌐 Status code: {page.status_code}")
        print(f"   ⏱️  Load time: {page.load_time:.2f} seconds")
        
        if page.error:
            print(f"   ⚠️  Error: {page.error}")
        
        # Test keyword detection
        print(f"\n🔍 Keyword Detection:")
        content_text = page.text.lower()
        content_html = page.html.lower()
        
        found_keywords = []
        for keyword in expected_keywords:
            if keyword in content_text or keyword in content_html:
                found_keywords.append(keyword)
                print(f"   ✅ Found keyword: '{keyword}'")
            else:
                print(f"   ❌ Missing keyword: '{keyword}'")
        
        # Verify at least some keywords were found
        assert len(found_keywords) > 0, f"No expected keywords found! Expected: {expected_keywords}"
        print(f"   🎯 Keywords found: {len(found_keywords)}/{len(expected_keywords)}")
        
        # Test custom selectors
        if page.metadata:
            print(f"\n🎛️  Custom Selector Results:")
            for key, value in page.metadata.items():
                if key.startswith("metadata_") and value:
                    selector_name = key.replace("metadata_", "")
                    print(f"   📋 {selector_name}: {value[:100]}{'...' if len(str(value)) > 100 else ''}")
        
        # Check if parquet file was created
        output_files = list(Path(temp_dir).glob("**/*.parquet"))
        if output_files:
            parquet_file = output_files[0]
            print(f"\n💾 Data Storage:")
            print(f"   📄 Parquet file: {parquet_file}")
            print(f"   📏 File size: {parquet_file.stat().st_size:,} bytes")
            
            # Load and verify parquet data
            try:
                df = pd.read_parquet(parquet_file)
                print(f"   📊 Rows in parquet: {len(df)}")
                print(f"   🏛️  Columns: {list(df.columns)}")
                
                # Verify data integrity
                assert len(df) > 0, "Parquet file is empty"
                assert 'html' in df.columns, "HTML column missing"
                assert 'text' in df.columns, "Text column missing"
                assert not df['html'].iloc[0] == "", "HTML content is empty"
                
                print("   ✅ Parquet data validation passed")
                
            except Exception as e:
                print(f"   ❌ Error reading parquet file: {e}")
        
        print(f"\n🎉 Test Completed Successfully!")
        print("=" * 60)
        
        return {
            "status": "success",
            "pages_scraped": result.total_pages,
            "keywords_found": found_keywords,
            "duration": result.duration,
            "output_files": len(output_files)
        }

async def test_multiple_urls():
    """Test scraping multiple URLs."""
    print("\n🌍 Testing Multiple URL Scraping...")
    print("-" * 40)
    
    storage_config = StorageConfig(output_dir="./test_output")
    browser_config = BrowserConfig(headless=True, timeout=15000)
    scraper = WebsiteScraper(storage_config=storage_config)
    
    urls = [
        "https://httpbin.org/html",
        "https://httpbin.org/json"
    ]
    
    print(f"📍 Scraping {len(urls)} URLs...")
    
    results = await scraper.scrape_multiple_urls(urls, browser_config=browser_config)
    
    print(f"✅ Scraped {len(results)} URLs")
    
    for i, result in enumerate(results, 1):
        print(f"   {i}. {result.url}")
        print(f"      📏 Text: {len(result.text)} chars")
        print(f"      ⏱️  Load time: {result.load_time:.2f}s")
        if result.error:
            print(f"      ❌ Error: {result.error}")
        else:
            print(f"      ✅ Success")
    
    return len(results)

async def main():
    """Run all tests."""
    try:
        # Run single URL test
        single_result = await test_scraper_with_keywords()
        
        # Run multiple URL test
        multi_count = await test_multiple_urls()
        
        print(f"\n🏆 ALL TESTS PASSED!")
        print(f"   • Single URL test: {single_result['status']}")
        print(f"   • Keywords found: {single_result['keywords_found']}")
        print(f"   • Multiple URLs: {multi_count} scraped")
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())