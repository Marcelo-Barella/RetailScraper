import json
import re
import scrapy
from urllib.parse import urlencode, urljoin
from helpers.helpers import extract_next_data
from scrapy import signals
from scrapy.exceptions import DontCloseSpider
import threading
from queue import Queue
import time
import random


class WalmartProductsParallelSpider(scrapy.Spider):
    """
    High-performance parallel spider that scrapes products for multiple stores simultaneously.
    Uses aggressive concurrency settings and parallel store processing.
    """
    name = 'walmart_products_parallel'
    
    custom_settings = {
        # Tuned settings for stealth and stability
        'CONCURRENT_REQUESTS': 32,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 32,
        'DOWNLOAD_DELAY': 0.5,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 5,
        'AUTOTHROTTLE_MAX_DELAY': 60,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 16,
        'REACTOR_THREADPOOL_MAXSIZE': 20,
        'CONCURRENT_ITEMS': 100,
        'RETRY_TIMES': 5,
        'DOWNLOAD_TIMEOUT': 30,
    }
    
    def __init__(self, stores_file='data/stores.jl', categories_file='data/categories.json', 
                 parallel_stores=10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stores_file = stores_file
        self.categories_file = categories_file
        self.parallel_stores = int(parallel_stores)  # Number of stores to process in parallel
        self.stores_queue = Queue()
        self.active_stores = 0
        self.lock = threading.Lock()
        self.stores = []
        self.categories = []
        self.processed_stores = set()
        self.store_category_status = {}  # Tracks pending categories for each store
        
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(WalmartProductsParallelSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider
        
    def spider_idle(self):
        """Called when spider runs out of requests"""
        with self.lock:
            # Check if we should schedule more stores
            should_schedule = self.active_stores < self.parallel_stores and not self.stores_queue.empty()

        if should_schedule:
            # Add a random delay (jitter) before scheduling the next batch of stores
            time.sleep(random.uniform(2, 5))
            # Start processing more stores
            for request in self.schedule_next_stores():
                self.crawler.engine.crawl(request, self)
            raise DontCloseSpider
    
    async def start(self):
        """Load stores and categories, then start parallel processing"""
        # Load stores
        try:
            with open(self.stores_file, 'r') as f:
                for line in f:
                    store = json.loads(line.strip())
                    if store.get('store_id') and store['store_id'] not in self.processed_stores:
                        self.stores_queue.put(store)
            self.logger.info(f"Loaded {self.stores_queue.qsize()} stores for processing")
        except Exception as e:
            self.logger.error(f"Failed to load stores: {e}")
            return
            
        # Load categories
        try:
            with open(self.categories_file, 'r') as f:
                self.categories = json.load(f)
            self.logger.info(f"Loaded {len(self.categories)} categories")
        except Exception as e:
            self.logger.error(f"Failed to load categories: {e}")
            return
            
        # Start processing multiple stores in parallel
        for request in self.schedule_next_stores():
            yield request
    
    def schedule_next_stores(self):
        """Schedule the next batch of stores for parallel processing"""
        scheduled = 0
        
        while self.active_stores < self.parallel_stores and not self.stores_queue.empty():
            try:
                store = self.stores_queue.get_nowait()
                if store['store_id'] not in self.processed_stores:
                    self.processed_stores.add(store['store_id'])
                    self.active_stores += 1
                    scheduled += 1
                    
                    self.logger.info(f"Starting store {store['store_id']} "
                                   f"({len(self.processed_stores)}/{len(self.processed_stores) + self.stores_queue.qsize()}) "
                                   f"- {self.active_stores} stores active")
                    
                    yield from self.set_store_cookie(store)
            except:
                break
                
        if scheduled > 0:
            self.logger.info(f"Scheduled {scheduled} new stores for processing")
    
    def set_store_cookie(self, store):
        """Set the store cookie by visiting the store page"""
        store_id = store['store_id']
        store_url = f"https://www.walmart.com/store/{store_id}"
        
        yield scrapy.Request(
            store_url,
            meta={
                'use_undetected_browser': True,
                'store': store,
                'category_index': 0,
                'cookiejar': store_id,
                'dont_retry': True,  # Skip retry for speed
            },
            callback=self.parse_store_page,
            errback=self.handle_store_error,
            dont_filter=True
        )
    
    def handle_store_error(self, failure):
        """Handle store setup errors"""
        store = failure.request.meta['store']
        self.logger.error(f"Failed to set store {store['store_id']}: {failure.value}")
        
        with self.lock:
            self.active_stores -= 1
            
        # Try next store
        yield from self.schedule_next_stores()
    
    def parse_store_page(self, response):
        """After setting store, start scraping all categories in parallel"""
        store = response.meta['store']
        
        if "store" in response.url:
            self.logger.info(f"Successfully set store {store['store_id']} - starting {len(self.categories)} categories")
            
            with self.lock:
                # Initialize category counter for the new active store
                self.store_category_status[store['store_id']] = len(self.categories)

            # Queue all categories for this store at once for parallel processing
            for idx, category in enumerate(self.categories):
                yield from self.scrape_category(store, category, page=1)
        else:
            self.logger.warning(f"Failed to set store {store['store_id']}")
            with self.lock:
                self.active_stores -= 1
            # We don't yield from schedule_next_stores() here anymore.
            # The spider_idle signal will handle scheduling new stores if needed.
    
    def scrape_category(self, store, category, page=1):
        """Scrape products from a category with in-store filter"""
        # Build the category URL with in-store filter
        params = {
            'affinityOverride': 'store_led',
            'stores': store['store_id'],
            'fulfillment': 'in_store',
            'page': page
        }
        
        category_url = f"https://www.walmart.com{category['path']}?{urlencode(params)}"
        
        yield scrapy.Request(
            category_url,
            meta={
                'use_undetected_browser': True,
                'store': store,
                'category': category,
                'page': page,
                'cookiejar': store['store_id'],
                'dont_retry': page > 1,  # Only retry first page
            },
            callback=self.parse_category,
            errback=self.handle_category_error,
            dont_filter=True
        )
    
    def handle_category_error(self, failure):
        """Handle category errors"""
        store = failure.request.meta['store']
        category = failure.request.meta['category']
        page = failure.request.meta['page']
        
        self.logger.error(f"Failed to scrape {category['name']} for store {store['store_id']} page {page}: {failure.value}")
        
        # Mark category as complete for this store to properly track progress
        self.category_complete_for_store(store)
    
    def parse_category(self, response):
        """Parse products from category page"""
        store = response.meta['store']
        category = response.meta['category']
        page = response.meta['page']
        
        # Extract product data from __NEXT_DATA__
        next_data = extract_next_data(response.text)
        if not next_data:
            if page == 1:
                self.logger.warning(f"No __NEXT_DATA__ found for {category['name']} in store {store['store_id']}")
            self.category_complete_for_store(store)
            return
            
        # Extract products
        products = self._extract_products(next_data)
        
        self.logger.info(f"Store {store['store_id']} - {category['name']} page {page}: {len(products)} products")
        
        for product in products:
            yield {
                'store_id': store['store_id'],
                'store_name': store.get('name', ''),
                'category': category['name'],
                'category_path': category['path'],
                'product_id': product.get('id'),
                'name': product.get('name'),
                'price': product.get('price'),
                'in_stock': product.get('availabilityStatus') == 'IN_STOCK',
                'brand': product.get('brand'),
                'image_url': product.get('imageInfo', {}).get('thumbnailUrl'),
                'product_url': f"https://www.walmart.com{product.get('canonicalUrl', '')}" if product.get('canonicalUrl') else None,
            }
        
        # Check if there are more pages
        if products and len(products) >= 40:  # Walmart typically shows 40 items per page
            # Request next page
            yield from self.scrape_category(store, category, page + 1)
        else:
            # Category complete for this store
            self.category_complete_for_store(store)
    
    def category_complete_for_store(self, store):
        """
        Decrements the category counter for a store. If all categories are done,
        it decrements the active_stores counter.
        """
        store_id = store['store_id']
        with self.lock:
            if store_id in self.store_category_status:
                self.store_category_status[store_id] -= 1
                if self.store_category_status[store_id] <= 0:
                    # All categories for this store are done
                    self.logger.info(f"Store {store_id} completed all {len(self.categories)} categories.")
                    if self.active_stores > 0:
                        self.active_stores -= 1
                    del self.store_category_status[store_id]
                    # The spider_idle signal will handle scheduling new stores if capacity allows.
            else:
                self.logger.warning(f"Store {store_id} not found in status tracker during completion check.")

    def _extract_products(self, next_data):
        """Extract product items from __NEXT_DATA__"""
        products = []
        
        try:
            # Navigate to the search results
            search_content = (
                next_data.get('props', {})
                .get('pageProps', {})
                .get('initialData', {})
                .get('searchResult', {})
                .get('itemStacks', [])
            )
            
            # Extract products from item stacks
            for stack in search_content:
                if stack.get('itemsV2'):
                    for item in stack['itemsV2']:
                        product_info = {}
                        
                        # Basic info
                        product_info['id'] = item.get('id')
                        product_info['name'] = item.get('name')
                        product_info['canonicalUrl'] = item.get('canonicalUrl')
                        product_info['brand'] = item.get('brand')
                        
                        # Price info
                        price_info = item.get('priceInfo', {})
                        if price_info.get('currentPrice'):
                            product_info['price'] = price_info['currentPrice'].get('price')
                        
                        # Availability
                        product_info['availabilityStatus'] = item.get('availabilityStatus')
                        
                        # Image
                        product_info['imageInfo'] = item.get('imageInfo', {})
                        
                        products.append(product_info)
                        
        except (KeyError, IndexError) as e:
            self.logger.warning(f"Could not extract product search results from __NEXT_DATA__: {e}")
            
        return products