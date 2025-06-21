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
    parser.add_argument(
        "--no-proxy",
        action="store_true",
        help="Run without using proxies. NOT RECOMMENDED for Walmart.",
    )
    args = parser.parse_args()

    # Clean up any leftover temp directories from previous runs
    cleanup_temp_directories()
    
    # Load our custom Scrapy settings
    settings = Settings()
    settings.setmodule(my_settings)
    
    # Base settings, optimized for scraping with good proxies
    spider_settings = {
        'CONCURRENT_REQUESTS': 10,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 10,
        'DOWNLOAD_DELAY': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 10,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 8.0,
        'RETRY_TIMES': 5,
    }
    
    # If --no-proxy is set, run with a single browser and disable proxies
    if args.no_proxy:
        print("--- Running without proxies ---")
        print("WARNING: This is not recommended for Walmart and will likely be blocked.")
        settings.set('USE_PROXY', False) # The middleware will see this and skip proxy logic if implemented
        spider_settings.update({
            'CONCURRENT_REQUESTS': 1,
            'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
            'DOWNLOAD_DELAY': 5,
            'AUTOTHROTTLE_TARGET_CONCURRENCY': 1.0,
        })
        # The new Hybrid middleware requires proxies, so we would need to switch it out
        # For simplicity, we assume the user will not use --no-proxy with the new setup.
        # If they do, it will fail gracefully in the middleware constructor.
    else:
        # This is the standard, recommended path
        print("--- Running with Oxylabs ISP proxies ---")
        settings.set('USE_PROXY', True)

    # Apply spider-specific settings
    settings.update(spider_settings)

    process = CrawlerProcess(settings)
    
    spider_to_run = None
    if args.find_stores:
        print("--- Finding all Walmart store locations ---")
        from crawlers.walmart_stores_spider import WalmartStoresSpider
        spider_to_run = WalmartStoresSpider
        settings.set('FEEDS', {'data/stores.json': {'format': 'json', 'overwrite': True}})
    elif args.find_categories:
        print("--- Finding all Walmart product categories ---")
        from crawlers.walmart_categories_spider import WalmartCategoriesSpider
        spider_to_run = WalmartCategoriesSpider
        settings.set('FEEDS', {'data/categories.json': {'format': 'json', 'overwrite': True}})
    elif args.scrape_products:
        print("--- Scraping products for all stores and categories ---")
        from crawlers.walmart_products_parallel_spider import WalmartProductsParallelSpider
        spider_to_run = WalmartProductsParallelSpider
        settings.set('FEEDS', {'data/products.jl': {'format': 'jsonlines', 'overwrite': False}})
        settings.set('DUPEFILTER_CLASS', 'scrapy.dupefilters.BaseDupeFilter')
    elif args.fetch_proxies:
        print("--- This script no longer supports fetching free proxies. ---")
        print("--- Please use your premium Oxylabs proxies. ---")
        return
    else:
        parser.print_help()
        return

    if spider_to_run:
        process.crawl(spider_to_run)
        process.start()
        print(f"--- Task complete ---")
    
if __name__ == "__main__":
    main() 