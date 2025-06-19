#!/usr/bin/env python
"""Quick test to see if improvements are working"""

import scrapy
from scrapy.crawler import CrawlerProcess
import random

class QuickTestSpider(scrapy.Spider):
    name = "quick_test"
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'COOKIES_ENABLED': True,
        'CONCURRENT_REQUESTS': 5,
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
    }
    
    def start_requests(self):
        # Test URLs
        test_urls = [
            "https://www.walmart.com/",
            "https://www.walmart.com/browse/electronics/3944",
            "https://www.walmart.com/ip/test/123456",  # Non-existent product
        ]
        
        for url in test_urls:
            # Add referrer
            headers = {}
            if random.random() < 0.7:  # 70% with referrer
                headers['Referer'] = random.choice([
                    "https://www.google.com/",
                    "https://www.walmart.com/",
                ])
            
            yield scrapy.Request(
                url,
                headers=headers,
                meta={'use_undetected_browser': True},
                callback=self.parse,
                errback=self.errback
            )
    
    def parse(self, response):
        self.logger.info(f"✅ SUCCESS: {response.url} (Proxy: {response.meta.get('proxy', 'None')})")
        
        # Check for bot detection
        if "robot" in response.text.lower() or "blocked" in response.url:
            self.logger.warning(f"⚠️ Possible bot detection at {response.url}")
        else:
            self.logger.info(f"✨ Clean response from {response.url}")
            
    def errback(self, failure):
        self.logger.error(f"❌ FAILED: {failure.request.url}")

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(QuickTestSpider)
    process.start()
