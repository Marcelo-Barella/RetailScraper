"""
Test script for enhanced Walmart scraper with improved proxy management
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import json
import logging
from scrapy import Spider, Request

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_enhanced_scraper():
    """Test the enhanced scraper with a few product URLs"""
    
    # Test URLs - mix of product and search pages
    test_urls = [
        "https://www.walmart.com/ip/VILINICE-Noise-Cancelling-Headphones-Wireless-Bluetooth-Over-Ear-Headphones-with-Microphone-Black/576467526",
        "https://www.walmart.com/search?q=laptops",
        "https://www.walmart.com/ip/Restored-Apple-MacBook-Pro-13-3-Intel-Core-i5-8GB-RAM-128GB-SSD-Mac-OS-Space-Gray-Refurbished/690933522"
    ]
    
    # Create a simple test spider
    class TestWalmartSpider(Spider):
        name = 'test_walmart'
        custom_settings = {
            'CONCURRENT_REQUESTS': 1,  # Test one at a time
            'LOG_LEVEL': 'INFO'
        }
        
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.results = []
        
        def start_requests(self):
            for url in test_urls:
                yield Request(
                    url,
                    meta={'use_undetected_browser': True},
                    callback=self.parse,
                    dont_filter=True
                )
        
        def parse(self, response):
            result = {
                'url': response.url,
                'status': response.status,
                'title': response.css('title::text').get(),
                'proxy_used': response.meta.get('proxy', 'No proxy'),
                'bot_detected': 'robot or human' in response.text.lower()
            }
            
            # Check for product data
            if '/ip/' in response.url:
                result['product_title'] = response.css('h1[itemprop="name"]::text').get()
                result['price'] = response.css('span[itemprop="price"]::text').get()
            
            self.results.append(result)
            logger.info(f"Scraped {response.url}: Bot detected = {result['bot_detected']}")
            
            # Save results
            with open('test_results.json', 'w') as f:
                json.dump(self.results, f, indent=2)
    
    # Get scrapy settings
    settings = get_project_settings()
    
    # Explicitly ensure our middleware is loaded
    print("\nConfigured Middlewares:")
    middlewares = settings.get('DOWNLOADER_MIDDLEWARES', {})
    for middleware, priority in sorted(middlewares.items(), key=lambda x: x[1]):
        print(f"  {middleware}: {priority}")
    print()
    
    # Create and run crawler
    process = CrawlerProcess(settings)
    process.crawl(TestWalmartSpider)
    process.start()
    
    # Print summary
    print("\n" + "="*60)
    print("TEST RESULTS SUMMARY")
    print("="*60)
    
    if os.path.exists('test_results.json'):
        with open('test_results.json', 'r') as f:
            results = json.load(f)
            
        for result in results:
            print(f"\nURL: {result['url']}")
            print(f"Status: {result['status']}")
            print(f"Proxy: {result['proxy_used']}")
            print(f"Bot Detected: {result['bot_detected']}")
            if result.get('product_title'):
                print(f"Product: {result['product_title']}")
                print(f"Price: {result.get('price', 'N/A')}")
    
    # Get proxy manager stats
    try:
        from helpers.enhanced_proxy_manager import EnhancedProxyManager
        pm = EnhancedProxyManager()
        stats = pm.get_stats_summary()
        
        print("\n" + "="*60)
        print("PROXY MANAGER STATS")
        print("="*60)
        print(json.dumps(stats, indent=2))
    except Exception as e:
        print(f"Could not get proxy stats: {e}")

if __name__ == "__main__":
    print("Testing Enhanced Walmart Scraper...")
    print("This will make real requests to Walmart.com")
    print("-" * 60)
    
    test_enhanced_scraper() 