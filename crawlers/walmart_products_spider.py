import json
import re
import scrapy
from urllib.parse import urlencode, urljoin
from helpers.helpers import extract_next_data


class WalmartProductsSpider(scrapy.Spider):
    """
    Spider that scrapes products for each Walmart store by:
    1. Setting the store as "My Store" (capturing location cookie)
    2. Scraping products from each category with in-store filter
    """
    name = 'walmart_products'
    
    def __init__(self, stores_file='data/stores.jl', categories_file='data/categories.json', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stores_file = stores_file
        self.categories_file = categories_file
        self.stores = []
        self.categories = []
        
    def start_requests(self):
        """Load stores and categories, then start the scraping process"""
        # Load stores
        try:
            with open(self.stores_file, 'r') as f:
                for line in f:
                    store = json.loads(line.strip())
                    if store.get('store_id'):
                        self.stores.append(store)
            self.logger.info(f"Loaded {len(self.stores)} stores")
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
            
        # Start with the first store
        if self.stores:
            yield from self.set_store_cookie(self.stores[0], store_index=0)
    
    def set_store_cookie(self, store, store_index):
        """Set the store cookie by visiting the store page"""
        store_id = store['store_id']
        store_url = f"https://www.walmart.com/store/{store_id}"
        
        yield scrapy.Request(
            store_url,
            meta={
                'use_undetected_browser': True,
                'store': store,
                'store_index': store_index,
                'category_index': 0,
                'cookiejar': store_id,  # Use store ID as cookie jar to maintain separate sessions
            },
            callback=self.parse_store_page,
            dont_filter=True
        )
    
    def parse_store_page(self, response):
        """After setting store, start scraping categories"""
        store = response.meta['store']
        store_index = response.meta['store_index']
        category_index = response.meta['category_index']
        
        # Check if we successfully set the store
        if "store" in response.url:
            self.logger.info(f"Successfully set store {store['store_id']} as current store")
            
            # Now scrape the first category for this store
            if category_index < len(self.categories):
                yield from self.scrape_category(store, store_index, category_index)
        else:
            self.logger.warning(f"Failed to set store {store['store_id']}")
            # Move to next store
            yield from self.next_store(store_index)
    
    def scrape_category(self, store, store_index, category_index):
        """Scrape products from a category with in-store filter"""
        if category_index >= len(self.categories):
            # Done with this store, move to next
            yield from self.next_store(store_index)
            return
            
        category = self.categories[category_index]
        
        # Build the category URL with in-store filter
        params = {
            'affinityOverride': 'store_led',  # Force store-specific results
            'stores': store['store_id'],
            'fulfillment': 'in_store',  # Only in-store items
            'page': 1
        }
        
        category_url = f"https://www.walmart.com{category['path']}?{urlencode(params)}"
        
        yield scrapy.Request(
            category_url,
            meta={
                'use_undetected_browser': True,
                'store': store,
                'store_index': store_index,
                'category': category,
                'category_index': category_index,
                'page': 1,
                'cookiejar': store['store_id'],
            },
            callback=self.parse_category,
            dont_filter=True
        )
    
    def parse_category(self, response):
        """Parse products from category page"""
        store = response.meta['store']
        store_index = response.meta['store_index']
        category = response.meta['category']
        category_index = response.meta['category_index']
        page = response.meta['page']
        
        self.logger.info(f"Parsing category {category['name']} for store {store['store_id']} (page {page})")
        
        # Extract product data from __NEXT_DATA__
        next_data = extract_next_data(response.text)
        if not next_data:
            self.logger.warning(f"No __NEXT_DATA__ found for category {category['name']}")
            # Move to next category
            yield from self.scrape_category(store, store_index, category_index + 1)
            return
            
        # Extract products
        products = self._extract_products(next_data)
        
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
            params = {
                'affinityOverride': 'store_led',
                'stores': store['store_id'],
                'fulfillment': 'in_store',
                'page': page + 1
            }
            next_url = f"https://www.walmart.com{category['path']}?{urlencode(params)}"
            
            yield scrapy.Request(
                next_url,
                meta={
                    'use_undetected_browser': True,
                    'store': store,
                    'store_index': store_index,
                    'category': category,
                    'category_index': category_index,
                    'page': page + 1,
                    'cookiejar': store['store_id'],
                },
                callback=self.parse_category,
                dont_filter=True
            )
        else:
            # Move to next category
            yield from self.scrape_category(store, store_index, category_index + 1)
    
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
                        
        except Exception as e:
            self.logger.error(f"Error extracting products: {e}")
            
        return products
    
    def next_store(self, store_index):
        """Move to the next store"""
        next_index = store_index + 1
        if next_index < len(self.stores):
            self.logger.info(f"Moving to store {next_index + 1}/{len(self.stores)}")
            yield from self.set_store_cookie(self.stores[next_index], next_index)
        else:
            self.logger.info("Completed scraping all stores") 
