#!/usr/bin/env python3
"""Simple test runner for the scraper."""
import asyncio
import sys
from pathlib import Path

# Add src to path so we can import scraper
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tests.test_scraper import test_keyword_search_simple

async def main():
    """Run the simple keyword test."""
    print("🚀 Starting scraper test...")
    print("=" * 50)
    
    try:
        # Run the test
        result = await test_keyword_search_simple()
        
        print("\n" + "=" * 50)
        print("🎉 Test completed successfully!")
        print(f"📄 Scraped URL: {result.url}")
        print(f"📝 Page title: {result.title}")
        print(f"⏱️  Load time: {result.load_time:.2f} seconds")
        print(f"📊 Status code: {result.status_code}")
        print(f"📏 Text length: {len(result.text):,} characters")
        print(f"🔗 Links found: {len(result.links)}")
        
        if result.error:
            print(f"⚠️  Error: {result.error}")
        else:
            print("✅ No errors detected")
            
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())