import os
import re
import scrapy
from scrapy import signals
from urllib.parse import urljoin
from helpers.helpers import extract_next_data, ensure_dir, DiscordProgressTracker
from middlewares import BotDetectionError

class WalmartStoresSpider(scrapy.Spider):
    """
    A Scrapy spider using a hybrid approach (undetected-chromedriver + HTTP requests)
    to crawl and scrape all Walmart store locations robustly.
    """
    name = 'walmart_stores'
    
    custom_settings = {
        # Ensure we don't stop on errors
        'CLOSESPIDER_ERRORCOUNT': 0,  # Don't close spider on errors
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 100,  # Keep retrying with different proxies
        # Discord webhook URL (loaded from config)
        'DISCORD_WEBHOOK_URL': '',  # Will be set in from_crawler
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.discord_tracker = None
        self.state_links_total = 0
        self.state_links_completed = 0
        self.failed_attempts = 0
        self.main_page_attempts = 0
        self.max_main_page_attempts = 1000  # Keep trying for a long time
        
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        
        # Load Discord webhook URL from config and initialize tracker
        try:
            from dotenv import load_dotenv
            load_dotenv(override=True)  # Load environment variables first
            
            from helpers.discord_config import get_webhook_url
            webhook_url = get_webhook_url()
            if webhook_url:
                spider.settings.set('DISCORD_WEBHOOK_URL', webhook_url)
                
                # Initialize Discord tracker immediately
                from helpers.helpers import ProxyManager, DiscordProgressTracker
                proxy_manager = ProxyManager()
                proxy_count = len(proxy_manager.proxies) if proxy_manager.proxies else 0
                
                spider.discord_tracker = DiscordProgressTracker(webhook_url)
                spider.discord_tracker.send_initial_embed(spider.name, spider.settings, proxy_count)
                print(f"Discord notifications enabled and initial message sent for {spider.name}")
            else:
                spider.discord_tracker = None
                print(f"Discord webhook not configured - notifications disabled for {spider.name}")
        except ImportError as e:
            spider.discord_tracker = None
            print(f"Discord config import failed: {e}")
        except Exception as e:
            spider.discord_tracker = None
            print(f"Discord configuration error: {e}")
            
        return spider
        
    def spider_opened(self, spider):
        """Called when spider is opened - Discord tracker already initialized in from_crawler."""
        if hasattr(self, 'discord_tracker') and self.discord_tracker:
            print(f"Spider {self.name} opened - Discord tracking active")
        else:
            print(f"Spider {self.name} opened - Discord tracking disabled")
    
    async def start(self):
        """
        Use the UndetectedBrowserMiddleware for the initial request to bypass the main block page.
        """
        # Send early Discord update that spider has started crawling
        if self.discord_tracker:
            self.log("Sending Discord update: Spider started crawling")
            self.discord_tracker.update_progress(
                "Starting crawler - fetching main directory page...",
                1,  # We don't know total yet
                0,
                0
            )
        
        # Start with the main directory request
        yield self._create_main_directory_request()

    def _create_main_directory_request(self):
        """Create a fresh request to the main directory page."""
        self.main_page_attempts += 1
        return scrapy.Request(
            "https://www.walmart.com/store-directory",
            meta={
                "use_undetected_browser": True,
                "main_page_attempt": self.main_page_attempts,
                "handle_httpstatus_list": [403, 503]  # Handle these status codes
            },
            callback=self.parse,
            errback=self.errback_httpbin,
            dont_filter=True  # Allow duplicate requests
        )

    def errback_httpbin(self, failure):
        """Handle errors and track failures for Discord updates."""
        self.failed_attempts += 1
        self.log(f'Request failed: {failure.request.url}')
        
        # Always send Discord update on errors, even if we don't have totals yet
        if self.discord_tracker:
            self.log("Sending Discord update: Request failed")
            total = self.state_links_total if self.state_links_total > 0 else 1
            self.discord_tracker.update_progress(
                f"ERROR: {failure.request.url} (Attempt {self.main_page_attempts})",
                total,
                self.state_links_completed,
                self.failed_attempts
            )
        
        # If this was a main directory request and we haven't exceeded max attempts, try again
        if "store-directory" in failure.request.url and not failure.request.url.count('/') > 4:
            if self.main_page_attempts < self.max_main_page_attempts:
                self.log(f"Main directory request failed, attempting again ({self.main_page_attempts}/{self.max_main_page_attempts})")
                yield self._create_main_directory_request()
            else:
                self.log("Exceeded maximum attempts for main directory page", level=scrapy.log.ERROR)

    def parse(self, response):
        """
        Parses the main directory page to find state links.
        All subsequent requests will also use browser automation for consistency.
        """
        self.log(f"Successfully parsed main directory page: {response.url}")
        
        # Log retry information
        retry_times = response.meta.get('retry_times', 0)
        main_page_attempt = response.meta.get('main_page_attempt', 0)
        if retry_times > 0 or main_page_attempt > 1:
            self.log(f"This is retry attempt #{retry_times} / main page attempt #{main_page_attempt} for {response.url}")
        
        # Check if we've been blocked
        if "/blocked" in response.url or "robot or human" in response.text.lower():
            self.log(f"Blocked on main directory page: {response.url}", level=scrapy.log.ERROR)
            
            # Instead of raising an exception, create a new request
            if self.main_page_attempts < self.max_main_page_attempts:
                self.log(f"Bot detection encountered, creating new request (attempt {self.main_page_attempts}/{self.max_main_page_attempts})")
                yield self._create_main_directory_request()
                return
            else:
                self.log("Exceeded maximum attempts due to bot detection", level=scrapy.log.ERROR)
                return
        
        # Save the response for debugging
        ensure_dir("debug")
        with open("debug/store_directory_page.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        self.log("Saved directory page to debug/store_directory_page.html for inspection")
        
        # Try multiple selectors to find state links
        state_links = []
        
        # Method 1: Direct CSS selector
        state_links.extend(response.css('a[href^="/store-directory/"]::attr(href)').getall())
        
        # Method 2: XPath with contains
        state_links.extend(response.xpath('//a[contains(@href, "/store-directory/")]/@href').getall())
        
        # Method 3: Look for links with state codes
        state_links.extend(response.xpath('//a[contains(@href, "/store-directory/") and string-length(@href) < 30]/@href').getall())
        
        # Method 4: Look in specific containers
        state_links.extend(response.css('.store-directory-container a::attr(href)').getall())
        state_links.extend(response.css('#maincontent a[href*="store-directory"]::attr(href)').getall())
        
        # Remove duplicates and filter
        state_links = list(set([link for link in state_links if link and '/store-directory/' in link and link != '/store-directory']))
        
        if not state_links:
            self.log("No state links found! Checking for alternative page structure...", level=scrapy.log.ERROR)
            
            # Check if we're on a different page or need JavaScript
            if "__NEXT_DATA__" in response.text:
                self.log("Found __NEXT_DATA__, extracting from JSON...")
                next_data = extract_next_data(response.text)
                if next_data:
                    # Try to find links in the JSON data
                    self._extract_links_from_json(next_data, state_links)
        
        self.log(f"Found {len(state_links)} state links to follow")
        
        # Send Discord update about state links found
        if self.discord_tracker:
            self.log("Sending Discord update: State links found")
            self.discord_tracker.update_progress(
                f"Found {len(state_links)} states to crawl",
                len(state_links) if state_links else 1,
                0,
                0
            )
        
        # If still no links, try the sitemap approach
        if not state_links:
            self.log("No state links found, trying sitemap...", level=scrapy.log.WARNING)
            yield scrapy.Request(
                "https://www.walmart.com/sitemap_store_main.xml",
                callback=self.parse_sitemap,
                meta={"use_undetected_browser": True},
                errback=self.errback_httpbin
            )
            return
        
        # Set total state links for progress tracking
        self.state_links_total = len(state_links)
        self.log(f"Discord tracking: Set total state links to {self.state_links_total}")
        
        # Initialize Discord progress tracking
        if self.discord_tracker:
            self.log("Initializing Discord progress tracking")
            self.discord_tracker.update_progress(
                "Starting state crawling...",
                self.state_links_total,
                0,
                0
            )
        
        # Follow each state link. For testing, you can add a slice like `[:5]`
        for state_link in state_links:
            self.log(f"Following state link: {state_link}")
            yield response.follow(
                state_link, 
                self.parse_state_or_city,
                meta={
                    'dont_retry': False,
                    'use_undetected_browser': True,  # Use browser for state pages too
                    'is_state_page': True  # Mark as state page for tracking
                },
                errback=self.errback_httpbin
            )

    def _extract_links_from_json(self, data, links_list):
        """Recursively extract links from JSON data"""
        if isinstance(data, dict):
            for key, value in data.items():
                if key in ['href', 'url', 'link'] and isinstance(value, str) and '/store-directory/' in value:
                    links_list.append(value)
                elif isinstance(value, (dict, list)):
                    self._extract_links_from_json(value, links_list)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    self._extract_links_from_json(item, links_list)

    def parse_sitemap(self, response):
        """Parse the sitemap as a fallback"""
        self.log("Parsing sitemap for store URLs...")
        
        # Check if we got blocked on sitemap too
        if "robot or human" in response.text.lower():
            self.log("Bot detection on sitemap, retrying main directory", level=scrapy.log.WARNING)
            if self.main_page_attempts < self.max_main_page_attempts:
                yield self._create_main_directory_request()
            return
        
        # Parse XML sitemap
        for url in response.xpath('//url/loc/text()').getall():
            if '/store/' in url and url.count('/') == 4:  # Direct store URLs
                store_id_match = re.search(r'/store/(\d+)', url)
                if store_id_match:
                    store_id = store_id_match.group(1)
                    yield scrapy.Request(
                        url,
                        meta={
                            "use_undetected_browser": True,
                            "href": url.replace('https://www.walmart.com', ''),
                            "store_id": store_id,
                            'dont_retry': False,
                        },
                        callback=self.parse_store,
                        errback=self.errback_httpbin
                    )

    def parse_state_or_city(self, response):
        """
        Parses state or city pages. Now using browser automation for all pages.
        """
        self.log(f"Parsing state/city page: {response.url}")
        
        # Track state page completion and update Discord
        if response.meta.get('is_state_page'):
            self.state_links_completed += 1
            self.log(f"Discord tracking: Completed {self.state_links_completed}/{self.state_links_total} state pages")
            if self.discord_tracker:
                self.log("Sending Discord progress update...")
                self.discord_tracker.update_progress(
                    response.url,
                    self.state_links_total,
                    self.state_links_completed,
                    self.failed_attempts
                )
                self.log("Discord progress update sent")
            else:
                self.log("Discord tracker not available for progress update", level=scrapy.log.WARNING)
        
        # Log retry information
        retry_times = response.meta.get('retry_times', 0)
        if retry_times > 0:
            self.log(f"This is retry attempt #{retry_times} for {response.url}")
        
        # Check if we've been redirected to a blocked page
        if "/blocked" in response.url or "robot or human" in response.text.lower():
            self.log(f"Blocked on state/city page: {response.url}", level=scrapy.log.ERROR)
            self.failed_attempts += 1
            if self.discord_tracker:
                self.discord_tracker.update_progress(
                    response.url,
                    self.state_links_total,
                    self.state_links_completed,
                    self.failed_attempts
                )
            # Don't raise exception, just skip this page
            return

        # Find city links
        city_links = response.css('a[href^="/store-directory/"]::attr(href)').getall()
        
        # Filter out self-references and already visited links
        current_path = response.url.split('walmart.com')[-1]
        new_city_links = [link for link in city_links if link != current_path]
        
        self.log(f"Found {len(new_city_links)} city links on {response.url}")
        
        for city_link in new_city_links:
            yield response.follow(
                city_link, 
                self.parse_state_or_city,
                meta={
                    'dont_retry': False,
                    'use_undetected_browser': True  # Use browser for city pages
                },
                errback=self.errback_httpbin
            )

        # Find store links on this page
        store_links = response.css('a[href^="/store/"]::attr(href)').getall()
        
        if store_links:
            self.log(f"Found {len(store_links)} stores on {response.url}")
        
        for store_link in store_links:
            # Extract store ID from URL (e.g., /store/288-woodville-tx -> 288)
            store_id_match = re.search(r'/store/(\d+)', store_link)
            store_id = store_id_match.group(1) if store_id_match else None
            
            yield scrapy.Request(
                response.urljoin(store_link),
                meta={
                    "use_undetected_browser": True,
                    "href": store_link,
                    "store_id": store_id,
                    'dont_retry': False,
                },
                callback=self.parse_store,
                errback=self.errback_httpbin
            )
            
    def parse_store(self, response):
        """
        Parses the final store page (from the browser middleware) to get the data.
        """
        self.log(f"Parsing store page from browser: {response.url}")
        
        # Log retry information
        retry_times = response.meta.get('retry_times', 0)
        if retry_times > 0:
            self.log(f"This is retry attempt #{retry_times} for {response.url}")
        
        href = response.meta["href"]
        store_id = response.meta.get("store_id")
        html_content = response.text
        
        # Check if we got a valid response
        if "Robot or human?" in html_content or "robot or human" in html_content.lower():
            self.log(f"Got CAPTCHA page for store {store_id}, skipping", level=scrapy.log.WARNING)
            # Don't yield anything, just skip this store
            return
        
        final_data = None
        store_data = extract_next_data(html_content)
        if store_data:
            final_data = (
                store_data.get("props", {})
                .get("pageProps", {})
                .get("initialData", {})
                .get("data", {})
                .get("store")
            )
        else:
            self.log(f"Could not find __NEXT_DATA__ on {href} despite using browser.", level=scrapy.log.WARNING)
            
        # Create a more structured item with store ID and URL
        item = {
            "store_id": store_id,
            "href": href,
            "url": response.url,
            "store_json": final_data
        }
        
        # Extract key information if available
        if final_data:
            item.update({
                "name": final_data.get("displayName"),
                "address": final_data.get("address"),
                "city": final_data.get("address", {}).get("city") if final_data.get("address") else None,
                "state": final_data.get("address", {}).get("state") if final_data.get("address") else None,
                "zip": final_data.get("address", {}).get("postalCode") if final_data.get("address") else None,
            })
            self.log(f"Successfully scraped store {store_id}: {item.get('name', 'Unknown')}")
        else:
            self.log(f"No store data found for {store_id}", level=scrapy.log.WARNING)
        
        yield item
    
    def spider_closed(self, spider, reason):
        """Called when spider closes."""
        if self.discord_tracker:
            # Count total items scraped from crawler stats
            stats = self.crawler.stats
            items_scraped = stats.get_value('item_scraped_count', 0)
            self.discord_tracker.send_completion_embed(self.name, items_scraped) 