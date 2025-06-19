import json
import scrapy
from helpers.helpers import extract_next_data


class WalmartCategoriesSpider(scrapy.Spider):
    """
    Spider that discovers all Walmart product categories by:
    1. Visiting the homepage
    2. Extracting category links from the navigation menu
    3. Following category pages to find subcategories
    """
    name = 'walmart_categories'
    start_urls = ['https://www.walmart.com']
    
    def __init__(self, max_depth=3, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_depth = int(max_depth)
        self.discovered_categories = set()
        
    async def start(self):
        """Start by visiting the homepage with browser"""
        yield scrapy.Request(
            self.start_urls[0],
            meta={'use_undetected_browser': True, 'depth': 0},
            callback=self.parse_homepage
        )
    
    def parse_homepage(self, response):
        """Extract main categories from homepage"""
        self.logger.info("Parsing Walmart homepage for categories")
        
        next_data = extract_next_data(response.text)
        if not next_data:
            self.logger.error("No __NEXT_DATA__ found on homepage")
            # Fallback to HTML-only parsing
        else:
            # First, try to get categories from the structured __NEXT_DATA__
            next_data_cats = self._extract_categories_from_next_data(next_data)
            next_data_links = [cat['path'] for cat in next_data_cats if cat.get('path')]
            yield from self._process_links(response, next_data_links, response.meta['depth'] + 1)
            
        # Also get categories from HTML links as a fallback and for additional coverage
        html_links = response.css('a[href*="/cp/"]::attr(href), a[href*="/browse/"]::attr(href)').getall()
        yield from self._process_links(response, html_links, response.meta['depth'] + 1)
    
    def parse_category(self, response):
        """Parse a category page to find subcategories"""
        depth = response.meta['depth']
        parent_category = response.meta['parent_category']
        
        self.logger.info(f"Parsing category: {parent_category} (depth: {depth})")
        
        # Look for subcategory links using both __NEXT_DATA__ and HTML
        next_data = extract_next_data(response.text)
        if next_data:
            next_data_cats = self._extract_categories_from_next_data(next_data)
            next_data_links = [cat['path'] for cat in next_data_cats if cat.get('path')]
            yield from self._process_links(response, next_data_links, depth + 1, parent_category)

        html_links = response.css('a[href*="/browse/"]::attr(href), a[href*="/cp/"]::attr(href)').getall()
        yield from self._process_links(response, html_links, depth + 1, parent_category)

    def _process_links(self, response, links, depth, parent_category=None):
        """
        Helper method to process a list of discovered category links,
        avoiding duplicates and yielding items/requests.
        """
        for link in links:
            # Ensure link is a full URL
            full_link = response.urljoin(link)
            path = full_link.split('walmart.com')[-1]

            if path not in self.discovered_categories:
                self.discovered_categories.add(path)
                category_name = self._extract_category_name(path)
                
                yield {
                    'name': category_name,
                    'path': path,
                    'level': depth,
                    'parent': parent_category
                }
                
                # Continue following if not at max depth
                if depth < self.max_depth:
                    yield response.follow(
                        path,
                        meta={
                            'use_undetected_browser': True,
                            'depth': depth + 1,
                            'parent_category': category_name,
                            'parent_path': path
                        },
                        callback=self.parse_category
                    )

    def _extract_categories_from_next_data(self, next_data):
        """Extract category information from __NEXT_DATA__"""
        categories = []
        
        # This is a simplified extraction - you may need to adjust based on actual structure
        try:
            # Look for department data in various possible locations
            props = next_data.get('props', {}).get('pageProps', {})
            
            # Check for department flyout data
            if 'initialData' in props:
                initial_data = props['initialData']
                if isinstance(initial_data, dict):
                    # Recursively search for category-like structures
                    self._find_categories_recursive(initial_data, categories)
                    
        except Exception as e:
            self.logger.error(f"Error extracting categories from __NEXT_DATA__: {e}")
            
        return categories
    
    def _find_categories_recursive(self, data, categories, depth=0):
        """Recursively search for category structures in data"""
        if depth > 10:  # Prevent infinite recursion
            return
            
        if isinstance(data, dict):
            # Look for category-like keys
            if 'departments' in data:
                for dept in data['departments']:
                    if isinstance(dept, dict) and 'link' in dept:
                        categories.append({
                            'name': dept.get('name', ''),
                            'path': dept.get('link', {}).get('href', '')
                        })
                        
            # Continue searching in nested structures
            for value in data.values():
                if isinstance(value, (dict, list)):
                    self._find_categories_recursive(value, categories, depth + 1)
                    
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    self._find_categories_recursive(item, categories, depth + 1)
    
    def _extract_category_name(self, url):
        """Extract a readable category name from URL"""
        # Remove query parameters
        path = url.split('?')[0]
        
        # Get the last segment
        segments = [s for s in path.split('/') if s]
        if segments:
            # Get the last segment and clean it up
            name = segments[-1]
            # Replace hyphens with spaces and title case
            name = name.replace('-', ' ').title()
            # Remove common suffixes
            name = name.replace(' Cp', '').replace(' Browse', '')
            return name
        
        return "Unknown Category" 