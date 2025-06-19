#!/usr/bin/env python3

import os
import sys
import argparse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.settings import Settings

# Load environment variables early
try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
    print("Environment variables loaded from .env file")
except ImportError:
    print("python-dotenv not installed - environment variables from system only")

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from helpers.helpers import cleanup_temp_directories
import scrapy_settings as my_settings

def main():
    parser = argparse.ArgumentParser(
        description="RetailScraper: A tool to scrape Walmart stores, categories, and products."
    )
    parser.add_argument(
        "--find-stores",
        action="store_true",
        help="Run the spider to find all Walmart store locations.",
    )
    parser.add_argument(
        "--find-categories",
        action="store_true",
        help="Run the spider to discover all Walmart product categories.",
    )
    parser.add_argument(
        "--scrape-products",
        action="store_true",
        help="Run the parallel spider to scrape products for each store and category.",
    )
    parser.add_argument(
        "--fetch-proxies",
        action="store_true",
        help="Fetch fresh free proxies from public sources and save to helpers/proxies.json.",
    )
    args = parser.parse_args()

    # Clean up any leftover temp directories from previous runs
    cleanup_temp_directories()
    
    # Load our custom Scrapy settings
    settings = Settings()
    settings.setmodule(my_settings)  # This loads our custom middlewares and settings
    
    # Spider-specific settings
    spider_settings = {
        # --- PRODUCTION SETTINGS ---
        # These are optimized for actual scraping with proxies
        'CONCURRENT_REQUESTS': 16,  # Increased from 3 for more parallelism
        'CONCURRENT_REQUESTS_PER_DOMAIN': 16,  # Match total concurrent requests
        'BROWSER_POOL_SIZE': 10,  # Increased from 3 - more browsers for parallel scraping
        'DOWNLOAD_DELAY': 1,  # Reduced from 3 seconds - adjust based on proxy quality
        'RANDOMIZE_DOWNLOAD_DELAY': True,  # Still randomize to appear more human
        'AUTOTHROTTLE_ENABLED': True,  # Keep autothrottle for adaptive speed
        'AUTOTHROTTLE_START_DELAY': 0.5,  # Start with shorter delay
        'AUTOTHROTTLE_MAX_DELAY': 10,  # Reduced from 30 for faster scraping
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 12.0,  # Increased from 3.0
        'AUTOTHROTTLE_DEBUG': False,
        
        # Retry settings remain aggressive for resilience
        'RETRY_TIMES': 100,
        'RETRY_ENABLED': True,
        
        # Memory and performance optimizations
        'CONCURRENT_ITEMS': 200,  # Process more items in parallel
        'REACTOR_THREADPOOL_MAXSIZE': 50,  # Increased thread pool
        
        # DNS and connection optimizations
        'DNSCACHE_ENABLED': True,
        'DNSCACHE_SIZE': 10000,
        'DNS_TIMEOUT': 60,
        'DOWNLOAD_TIMEOUT': 30,  # Slightly increased for browser operations
    }
    
    # Apply spider-specific settings
    settings.update(spider_settings)

    if args.find_stores:
        print("--- Finding all Walmart store locations ---")
        
        # Configure output for stores
        settings.set('FEEDS', {
            'data/stores.json': {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
                'overwrite': True,
            }
        })

        process = CrawlerProcess(settings)
        from crawlers.walmart_stores_spider import WalmartStoresSpider
        process.crawl(WalmartStoresSpider)
        process.start()
        print("--- Store finding complete ---")

    elif args.find_categories:
        print("--- Finding all Walmart product categories ---")
        
        # Configure output for categories
        settings.set('FEEDS', {
            'data/categories.json': {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
                'overwrite': True,
            }
        })

        process = CrawlerProcess(settings)
        from crawlers.walmart_categories_spider import WalmartCategoriesSpider
        process.crawl(WalmartCategoriesSpider)
        process.start()
        print("--- Category finding complete ---")

    elif args.scrape_products:
        print("--- Scraping products for all stores and categories ---")
        
        # Configure output for products
        settings.set('FEEDS', {
            'data/products.jl': {
                'format': 'jsonlines',
                'encoding': 'utf8',
                'store_empty': False,
                'overwrite': False,
            }
        })
        
        # Disable duplicate filtering for products since we want all store/category combinations
        settings.set('DUPEFILTER_CLASS', 'scrapy.dupefilters.BaseDupeFilter')

        process = CrawlerProcess(settings)
        from crawlers.walmart_products_parallel_spider import WalmartProductsParallelSpider
        process.crawl(WalmartProductsParallelSpider)
        process.start()
        print("--- Product scraping complete ---")

    elif args.fetch_proxies:
        print("--- Fetching fresh free proxies ---")
        print("This will collect proxies from multiple public sources...")
        
        # Use lighter settings for proxy collection
        proxy_settings = Settings()
        proxy_settings.setmodule(my_settings)
        proxy_settings.update({
            'CONCURRENT_REQUESTS': 8,
            'DOWNLOAD_DELAY': 0.5,
            'LOG_LEVEL': 'INFO',
            'RETRY_ENABLED': False,
            'FEEDS': {},  # No feeds needed, spider handles JSON output
        })

        process = CrawlerProcess(proxy_settings)
        from crawlers.free_proxy_spider import FreeProxySpider
        process.crawl(FreeProxySpider)
        process.start()
        print("--- Proxy fetching complete ---")
        print("\nNext steps:")
        print("1. Test the proxies with: python main.py --find-stores")
        print("2. If needed, get better residential proxies for production use")

    else:
        # If no arguments provided, show help
        parser.print_help()
        print("\nExample usage:")
        print("  python main.py --fetch-proxies     # Get fresh free proxies")
        print("  python main.py --find-stores       # Scrape store locations")
        print("  python main.py --find-categories   # Discover product categories") 
        print("  python main.py --scrape-products   # Scrape all products")

if __name__ == "__main__":
    main() 