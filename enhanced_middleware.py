import logging
import os
import queue
import shutil
import tempfile
import threading
import time
import random
import subprocess
import psutil
import json
from datetime import datetime

from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from twisted.internet import defer
from twisted.python.failure import Failure

from helpers.adaptive_proxy_manager import AdaptiveProxyManager
from helpers.config import TEMP_BROWSER_SESSIONS_POOL_DIR
from middlewares import BotDetectionError, kill_chrome_processes
from helpers.enhanced_proxy_manager import EnhancedProxyManager

logger = logging.getLogger(__name__)

# Expanded user agent list with recent versions
ENHANCED_USER_AGENTS = [
    # Windows - Chrome latest versions
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    
    # Windows - Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    
    # Windows - Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    
    # macOS - Chrome
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    
    # macOS - Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    
    # Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
]

# Navigation patterns
NAVIGATION_PATTERNS = {
    "direct": {
        "weight": 0.3,
        "referrer": None
    },
    "google_search": {
        "weight": 0.4,
        "referrer": "https://www.google.com/"
    },
    "walmart_internal": {
        "weight": 0.2,
        "referrer": "https://www.walmart.com/"
    },
    "social_media": {
        "weight": 0.1,
        "referrer": random.choice([
            "https://www.facebook.com/",
            "https://twitter.com/",
            "https://www.pinterest.com/"
        ])
    }
}

class HumanBehaviorSimulator:
    """Simulates human-like browsing behavior"""
    
    @staticmethod
    def get_mouse_path(start_x, start_y, end_x, end_y, points=5):
        """Generate a curved mouse path between two points"""
        path = []
        for i in range(points):
            t = i / (points - 1)
            # Add some curve with sine wave
            offset = random.randint(-50, 50) * (1 - abs(t - 0.5) * 2)
            x = start_x + (end_x - start_x) * t + offset
            y = start_y + (end_y - start_y) * t + offset * 0.5
            path.append((int(x), int(y)))
        return path
    
    @staticmethod
    def human_scroll(driver):
        """Perform human-like scrolling"""
        # Get page height
        page_height = driver.execute_script("return document.body.scrollHeight")
        viewport_height = driver.execute_script("return window.innerHeight")
        
        # Decide scroll behavior
        scroll_patterns = [
            "smooth_down",  # Smooth scroll down
            "chunky_down",  # Scroll in chunks
            "scan_and_return",  # Scroll down and back up
            "quick_scan"  # Quick scroll to check page
        ]
        
        pattern = random.choice(scroll_patterns)
        
        if pattern == "smooth_down":
            # Smooth progressive scroll
            current_position = 0
            while current_position < page_height - viewport_height:
                scroll_amount = random.randint(100, 300)
                driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(random.uniform(0.5, 1.5))
                current_position += scroll_amount
                
                # Sometimes pause to "read"
                if random.random() < 0.3:
                    time.sleep(random.uniform(1, 3))
        
        elif pattern == "chunky_down":
            # Scroll in larger chunks
            chunks = random.randint(3, 5)
            for i in range(chunks):
                scroll_to = (page_height / chunks) * (i + 1)
                driver.execute_script(f"window.scrollTo(0, {scroll_to});")
                time.sleep(random.uniform(1, 2))
        
        elif pattern == "scan_and_return":
            # Scroll down then back up (like looking for something)
            driver.execute_script(f"window.scrollTo(0, {page_height * 0.7});")
            time.sleep(random.uniform(1, 2))
            driver.execute_script(f"window.scrollTo(0, {page_height * 0.3});")
            time.sleep(random.uniform(0.5, 1))
        
        else:  # quick_scan
            # Quick scroll to bottom and top
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(0.5, 1))
            driver.execute_script("window.scrollTo(0, 0);")
    
    @staticmethod
    def human_mouse_movement(driver):
        """Simulate human-like mouse movements"""
        from selenium.webdriver.common.action_chains import ActionChains
        
        actions = ActionChains(driver)
        viewport_width = driver.execute_script("return window.innerWidth;")
        viewport_height = driver.execute_script("return window.innerHeight;")
        
        # Different movement patterns
        patterns = ["reading", "searching", "hovering", "wandering"]
        pattern = random.choice(patterns)
        
        if pattern == "reading":
            # Simulate reading pattern (left to right, top to bottom)
            for _ in range(random.randint(3, 6)):
                start_x = random.randint(100, viewport_width // 3)
                start_y = random.randint(100, viewport_height - 100)
                end_x = random.randint(viewport_width * 2 // 3, viewport_width - 100)
                end_y = start_y + random.randint(-50, 50)
                
                path = HumanBehaviorSimulator.get_mouse_path(start_x, start_y, end_x, end_y)
                for x, y in path:
                    actions.move_by_offset(x - actions._pointer_location.x, 
                                         y - actions._pointer_location.y)
                    actions.pause(random.uniform(0.01, 0.05))
                actions.perform()
                actions.reset_actions()
                time.sleep(random.uniform(0.5, 1))
        
        elif pattern == "searching":
            # Quick movements like searching for something
            for _ in range(random.randint(5, 10)):
                x = random.randint(100, viewport_width - 100)
                y = random.randint(100, viewport_height - 100)
                actions.move_by_offset(x - actions._pointer_location.x,
                                     y - actions._pointer_location.y)
                actions.pause(random.uniform(0.1, 0.3))
                actions.perform()
                actions.reset_actions()
        
        elif pattern == "hovering":
            # Hover over elements (simulating interest)
            try:
                elements = driver.find_elements_by_css_selector("a, button, img")[:10]
                for element in random.sample(elements, min(3, len(elements))):
                    actions.move_to_element(element)
                    actions.pause(random.uniform(0.5, 2))
                    actions.perform()
                    actions.reset_actions()
            except:
                pass
        
        else:  # wandering
            # Random wandering movement
            for _ in range(random.randint(2, 4)):
                x = random.randint(100, viewport_width - 100)
                y = random.randint(100, viewport_height - 100)
                path = HumanBehaviorSimulator.get_mouse_path(
                    actions._pointer_location.x, actions._pointer_location.y, x, y
                )
                for px, py in path:
                    actions.move_by_offset(px - actions._pointer_location.x,
                                         py - actions._pointer_location.y)
                    actions.pause(random.uniform(0.02, 0.08))
                actions.perform()
                actions.reset_actions()
                time.sleep(random.uniform(0.5, 1.5))


class EnhancedProxyBrowserMiddleware:
    """Enhanced middleware with advanced anti-detection features"""
    
    MAX_BROWSER_RETRIES = 10
    _init_lock = threading.Lock()
    
    def __init__(self, settings):
        # Use the enhanced proxy manager for Walmart
        self.proxy_manager = EnhancedProxyManager()
        
        self.browser_pool_size = settings.getint('BROWSER_POOL_SIZE', 
                                               settings.getint('CONCURRENT_REQUESTS', 10))
        self.browser_pool = None
        self.user_data_dirs = []
        self.browser_sessions = {}  # Track browser sessions
        
        # Use timestamp for unique directory
        timestamp = int(time.time() * 1000)
        self.sessions_base_dir = os.path.join(
            os.path.dirname(TEMP_BROWSER_SESSIONS_POOL_DIR), 
            f'enhanced_browser_pool_{timestamp}'
        )
        
        # Clean up old directories
        self._cleanup_old_sessions()
        
        # Create new directory
        os.makedirs(self.sessions_base_dir, exist_ok=True)
        logger.info(f"Enhanced browser pool using: {self.sessions_base_dir}")
        
        # Print proxy summary
        stats = self.proxy_manager.get_stats_summary()
        logger.info(f"Proxy manager initialized - Residential: {stats['residential_proxies']}, "
                   f"Mobile: {stats['mobile_proxies']}, Datacenter: {stats['datacenter_proxies']}")
    
    def _cleanup_old_sessions(self):
        """Clean up old session directories"""
        base_dir = os.path.dirname(TEMP_BROWSER_SESSIONS_POOL_DIR)
        if os.path.exists(base_dir):
            for item in os.listdir(base_dir):
                if item.startswith('enhanced_browser_pool_'):
                    old_dir = os.path.join(base_dir, item)
                    try:
                        shutil.rmtree(old_dir)
                        logger.info(f"Cleaned up old session: {old_dir}")
                    except:
                        pass
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler.settings)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware
    
    def spider_opened(self, spider):
        """Initialize browser pool with enhanced configuration"""
        self.browser_pool = queue.Queue(maxsize=self.browser_pool_size)
        logger.info(f"Creating enhanced browser pool with {self.browser_pool_size} instances...")
        
        threads = []
        for i in range(self.browser_pool_size):
            # Get proxy with context using enhanced proxy manager
            proxy = self.proxy_manager.get_proxy({"initialization": True})
            thread = threading.Thread(target=self._create_enhanced_browser, args=(i, proxy))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        if self.browser_pool.empty():
            raise RuntimeError("Failed to initialize browser pool")
        
        logger.info(f"Enhanced browser pool ready with {self.browser_pool.qsize()} instances")
    
    def _create_enhanced_browser(self, index, proxy):
        """Create browser with enhanced anti-detection features"""
        try:
            user_data_dir = tempfile.mkdtemp(
                prefix=f"enhanced_browser_{index}_", 
                dir=self.sessions_base_dir
            )
            self.user_data_dirs.append(user_data_dir)
            
            driver = self._create_advanced_browser(user_data_dir, proxy)
            if driver:
                # Get proxy location for timezone matching
                proxy_details = {}
                # Find proxy details from our loaded proxies
                for p in self.proxy_manager.all_proxies:
                    if p.get("proxy") == proxy:
                        proxy_details = p
                        break
                location = proxy_details.get("location", {})
                
                browser_info = {
                    "driver": driver,
                    "proxy": proxy,
                    "user_data_dir": user_data_dir,
                    "warmed_up": False,
                    "session_start": datetime.now(),
                    "request_count": 0,
                    "location": location,
                    "user_agent": driver.execute_script("return navigator.userAgent"),
                    "session_id": f"session_{index}_{int(time.time())}"
                }
                
                self.browser_pool.put(browser_info)
                self.browser_sessions[browser_info["session_id"]] = browser_info
        except Exception as e:
            logger.error(f"Error creating enhanced browser: {e}", exc_info=True)
    
    def _create_advanced_browser(self, user_data_dir, proxy):
        """Create browser with advanced configuration"""
        try:
            import undetected_chromedriver as uc
            from selenium.webdriver.common.proxy import Proxy, ProxyType
            
            options = uc.ChromeOptions()
            options.add_argument('--headless=new')
            
            # Get location-specific settings
            proxy_details = {}
            # Find proxy details from our loaded proxies
            for p in self.proxy_manager.all_proxies:
                if p.get("proxy") == proxy:
                    proxy_details = p
                    break
            location = proxy_details.get("location", {})
            
            # Select user agent based on proxy location
            if location.get("countryCode") == "US":
                # Use US-specific user agents
                user_agent = random.choice([ua for ua in ENHANCED_USER_AGENTS if "Windows" in ua or "Macintosh" in ua])
            else:
                user_agent = random.choice(ENHANCED_USER_AGENTS)
            
            options.add_argument(f'--user-agent={user_agent}')
            
            # Language based on location
            if location.get("countryCode") == "US":
                options.add_argument('--lang=en-US')
                options.add_argument('--accept-lang=en-US,en;q=0.9')
            else:
                # Use appropriate language for country
                options.add_argument('--lang=en-US')
                options.add_argument('--accept-lang=en-US,en;q=0.8')
            
            # Random but consistent window size
            window_sizes = ["1920,1080", "1366,768", "1536,864", "1440,900", "1280,720", "2560,1440"]
            options.add_argument(f'--window-size={random.choice(window_sizes)}')
            
            # Enhanced anti-detection arguments
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            # Additional privacy/security options
            options.add_argument("--disable-plugins-discovery")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-setuid-sandbox")
            options.add_argument("--disable-web-security")
            options.add_argument("--allow-running-insecure-content")
            
            # Memory optimization
            options.add_argument("--memory-pressure-off")
            options.add_argument("--force-device-scale-factor=1")
            
            # Set proxy
            if proxy:
                proxy_address = proxy.split('://', 1)[-1]
                selenium_proxy = Proxy()
                selenium_proxy.proxy_type = ProxyType.MANUAL
                selenium_proxy.http_proxy = proxy_address
                selenium_proxy.ssl_proxy = proxy_address
                options.proxy = selenium_proxy
            
            with self._init_lock:
                driver = uc.Chrome(options=options, user_data_dir=user_data_dir, version_main=None)
            
            # Advanced JavaScript injection for fingerprinting
            self._inject_advanced_fingerprint_protection(driver, location)
            
            # Set proper page load timeout
            driver.set_page_load_timeout(90)
            
            return driver
            
        except Exception as e:
            logger.error(f"Failed to create browser with proxy {proxy}: {e}", exc_info=True)
            return None
    
    def _inject_advanced_fingerprint_protection(self, driver, location):
        """Inject advanced fingerprint protection"""
        # Timezone spoofing based on proxy location
        timezone = location.get("timezone", "America/New_York")
        
        js_code = f"""
        // Override timezone
        Object.defineProperty(Intl.DateTimeFormat.prototype, 'resolvedOptions', {{
            value: function() {{
                return {{
                    timeZone: '{timezone}',
                    calendar: 'gregory',
                    numberingSystem: 'latn',
                    locale: 'en-US'
                }};
            }}
        }});
        
        // WebGL fingerprint protection
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {{
            if (parameter === 37445) {{
                return 'Intel Inc.';
            }}
            if (parameter === 37446) {{
                return 'Intel Iris OpenGL Engine';
            }}
            return getParameter.apply(this, arguments);
        }};
        
        // Canvas fingerprint protection
        const toDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function() {{
            const dataURL = toDataURL.apply(this, arguments);
            // Add small random noise
            return dataURL.substring(0, dataURL.length - 1) + Math.random().toString(36).substring(7, 8);
        }};
        
        // Audio fingerprint protection
        const createAnalyser = AudioContext.prototype.createAnalyser;
        AudioContext.prototype.createAnalyser = function() {{
            const analyser = createAnalyser.apply(this, arguments);
            const getFloatFrequencyData = analyser.getFloatFrequencyData;
            analyser.getFloatFrequencyData = function(array) {{
                getFloatFrequencyData.apply(this, arguments);
                for (let i = 0; i < array.length; i++) {{
                    array[i] = array[i] + Math.random() * 0.1;
                }}
            }};
            return analyser;
        }};
        
        // Battery API protection
        delete navigator.getBattery;
        
        // WebRTC protection
        const RTCPeerConnection = window.RTCPeerConnection || window.webkitRTCPeerConnection;
        if (RTCPeerConnection) {{
            const original = RTCPeerConnection.prototype.createDataChannel;
            RTCPeerConnection.prototype.createDataChannel = function() {{
                return null;
            }};
        }}
        
        // Plugin detection evasion
        Object.defineProperty(navigator, 'plugins', {{
            get: () => {{
                const plugins = [
                    {{name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer'}},
                    {{name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai'}},
                    {{name: 'Native Client', filename: 'internal-nacl-plugin'}}
                ];
                plugins.length = plugins.length;
                return plugins;
            }}
        }});
        
        // More realistic navigator properties
        Object.defineProperty(navigator, 'hardwareConcurrency', {{get: () => 4 + Math.floor(Math.random() * 4)}});
        Object.defineProperty(navigator, 'deviceMemory', {{get: () => 8}});
        Object.defineProperty(navigator, 'maxTouchPoints', {{get: () => 0}});
        
        // Notification permission
        const originalQuery = window.Notification.requestPermission;
        window.Notification.requestPermission = function() {{
            return Promise.resolve('denied');
        }};
        """
        
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': js_code})
    
    def process_request(self, request, spider):
        """Process request with enhanced anti-detection"""
        if not request.meta.get("use_undetected_browser"):
            return None
        
        # Get proxy considering request context using enhanced proxy manager
        context = {
            "url": request.url,
            "url_type": self._get_url_type(request.url),
            "retry_count": request.meta.get('retry_count', 0),
            "last_proxy": request.meta.get('proxy')
        }
        
        try:
            browser_info = self.browser_pool.get(timeout=300)
            
            # Check if we should rotate proxy for this browser
            if browser_info.get("request_count", 0) > 10 or request.meta.get('retry_count', 0) > 0:
                # Get new proxy from enhanced proxy manager
                old_proxy = browser_info["proxy"]
                new_proxy = self.proxy_manager.get_proxy(context)
                
                if new_proxy and new_proxy != old_proxy:
                    logger.info(f"Rotating proxy from {old_proxy} to {new_proxy}")
                    # Close old browser and create new one with new proxy
                    try:
                        browser_info["driver"].quit()
                    except:
                        pass
                    
                    # Create new browser with new proxy
                    driver = self._create_advanced_browser(browser_info["user_data_dir"], new_proxy)
                    if driver:
                        browser_info["driver"] = driver
                        browser_info["proxy"] = new_proxy
                        browser_info["request_count"] = 0
                        browser_info["warmed_up"] = False
            
            browser_info["request_count"] = browser_info.get("request_count", 0) + 1
            
            # Set proxy in request meta
            request.meta['proxy'] = browser_info["proxy"]
            request.meta['browser_info'] = browser_info
            
            deferred = defer.maybeDeferred(self._execute_enhanced_request, request, browser_info)
            deferred.addBoth(self._release_enhanced_browser, browser_info)
            
            return deferred
            
        except queue.Empty:
            raise IgnoreRequest("Timed out waiting for browser from pool")
    
    def _get_url_type(self, url):
        """Categorize URL type"""
        if "/ip/" in url:
            return "product"
        elif "/browse/" in url or "/cp/" in url:
            return "category"
        elif "/search/" in url:
            return "search"
        elif "/store/" in url:
            return "store"
        else:
            return "other"
    
    def _execute_enhanced_request(self, request, browser_info):
        """Execute request with enhanced human-like behavior"""
        driver = browser_info["driver"]
        proxy = browser_info["proxy"]
        start_time = time.time()
        
        logger.info(f"[Enhanced Browser] Using proxy {proxy} for {request.url}")
        
        try:
            # Warm-up with human-like behavior
            if not browser_info["warmed_up"]:
                self._warm_up_browser(driver, browser_info)
                browser_info["warmed_up"] = True
            
            # Select navigation pattern
            nav_pattern = self._select_navigation_pattern()
            
            # Set referrer if applicable
            if nav_pattern["referrer"]:
                driver.execute_script(f"window.history.pushState(null, '', '{nav_pattern['referrer']}');")
                time.sleep(random.uniform(0.5, 1))
            
            # Add random pre-navigation delay
            optimal_interval = random.uniform(1.5, 3.5)  # Use random interval between 1.5 and 3.5 seconds
            time.sleep(random.uniform(optimal_interval * 0.8, optimal_interval * 1.2))
            
            # Add referrer for more natural navigation
            if nav_pattern["referrer"]:
                driver.execute_script(f"document.referrer = '{nav_pattern['referrer']}';")
            
            # Navigate to page
            driver.get(request.url)
            
            # Wait for page load with human-like behavior
            self._human_like_wait(driver)
            
            # Perform human-like interactions
            HumanBehaviorSimulator.human_mouse_movement(driver)
            HumanBehaviorSimulator.human_scroll(driver)
            
            # Additional interactions based on page type
            url_type = self._get_url_type(request.url)
            if url_type == "product":
                self._interact_with_product_page(driver)
            elif url_type == "category":
                self._interact_with_category_page(driver)
            
            # Check for bot detection
            if self._check_bot_detection(driver):
                logger.warning(f"Bot detection encountered with proxy {proxy}")
                self.proxy_manager.record_failure(proxy, bot_detected=True)
                raise BotDetectionError("Bot detection page encountered")
            
            # Record success
            response_time = time.time() - start_time
            self.proxy_manager.record_success(
                proxy, 
                response_time=response_time
            )
            
            # Update session data
            session_data = {
                "last_success": datetime.now().isoformat(),
                "pages_visited": browser_info.get("pages_visited", 0) + 1,
                "total_time": (datetime.now() - browser_info["session_start"]).total_seconds(),
                "timestamp": datetime.now().isoformat()
            }
            # Store session data in browser_info instead
            browser_info.update(session_data)
            
            return HtmlResponse(
                request.url,
                body=driver.page_source.encode('utf-8'),
                encoding='utf-8',
                request=request,
                status=200
            )
            
        except BotDetectionError:
            raise
        except Exception as e:
            logger.error(f"Enhanced request failed with proxy {proxy}: {e}")
            self.proxy_manager.record_failure(proxy)
            raise
    
    def _warm_up_browser(self, driver, browser_info):
        """Warm up browser with realistic behavior"""
        logger.info("Warming up browser session...")
        
        # Visit homepage
        driver.get("https://www.walmart.com/")
        time.sleep(random.uniform(3, 5))
        
        # Perform initial interactions
        HumanBehaviorSimulator.human_mouse_movement(driver)
        HumanBehaviorSimulator.human_scroll(driver)
        
        # Sometimes click on a random link
        if random.random() < 0.3:
            try:
                links = driver.find_elements_by_css_selector("a[href*='/browse/']")[:5]
                if links:
                    random.choice(links).click()
                    time.sleep(random.uniform(2, 4))
                    driver.back()
                    time.sleep(random.uniform(1, 2))
            except:
                pass
    
    def _human_like_wait(self, driver):
        """Wait for page load with human-like timing"""
        # Initial wait
        time.sleep(random.uniform(1, 2))
        
        # Wait for document ready
        for _ in range(10):
            ready_state = driver.execute_script("return document.readyState")
            if ready_state == "complete":
                break
            time.sleep(0.5)
        
        # Additional wait based on page complexity
        try:
            elements_count = len(driver.find_elements_by_css_selector("*"))
            if elements_count > 500:
                time.sleep(random.uniform(1, 2))
        except:
            pass
    
    def _interact_with_product_page(self, driver):
        """Interact with product page like a human"""
        try:
            # Sometimes click on product images
            if random.random() < 0.3:
                images = driver.find_elements_by_css_selector("img[src*='product']")[:3]
                if images:
                    random.choice(images).click()
                    time.sleep(random.uniform(0.5, 1))
            
            # Sometimes hover over buttons
            if random.random() < 0.4:
                buttons = driver.find_elements_by_css_selector("button")[:5]
                if buttons:
                    from selenium.webdriver.common.action_chains import ActionChains
                    actions = ActionChains(driver)
                    actions.move_to_element(random.choice(buttons)).perform()
                    time.sleep(random.uniform(0.5, 1))
        except:
            pass
    
    def _interact_with_category_page(self, driver):
        """Interact with category page like a human"""
        try:
            # Sometimes hover over products
            if random.random() < 0.5:
                products = driver.find_elements_by_css_selector("[data-item-id]")[:10]
                if products:
                    from selenium.webdriver.common.action_chains import ActionChains
                    actions = ActionChains(driver)
                    for _ in range(random.randint(1, 3)):
                        actions.move_to_element(random.choice(products)).perform()
                        time.sleep(random.uniform(0.5, 1.5))
        except:
            pass
    
    def _check_bot_detection(self, driver):
        """Check for bot detection indicators"""
        try:
            # Check page title
            title = driver.title.lower()
            
            # Check page source
            page_source = driver.page_source.lower()
            
            # Check current URL for /blocked pattern
            current_url = driver.current_url.lower()
            
            # Common bot detection indicators
            bot_indicators = [
                "robot or human",
                "are you a robot",
                "access denied",
                "please verify",
                "unusual traffic",
                "suspicious activity",
                "bot detection",
                "security check",
                "challenge",
                "verify you're human"
            ]
            
            # Check title, page source, and URL
            for indicator in bot_indicators:
                if indicator in title or indicator in page_source:
                    logger.warning(f"Bot detection indicator found: '{indicator}'")
                    return True
            
            # Check for /blocked in URL
            if "/blocked" in current_url:
                logger.warning(f"Detected blocked URL redirect: {current_url}")
                return True
            
            # Check for specific Walmart/PerimeterX elements
            try:
                # Check for PerimeterX challenge
                if driver.find_elements_by_css_selector("#px-captcha"):
                    logger.warning("PerimeterX CAPTCHA detected")
                    return True
                
                # Check for press and hold button
                if driver.find_elements_by_xpath("//*[contains(text(), 'Press & Hold')]"):
                    logger.warning("Press & Hold challenge detected")
                    return True
            except:
                pass
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking bot detection: {e}")
            return False
    
    def _select_navigation_pattern(self):
        """Select navigation pattern based on weights"""
        patterns = list(NAVIGATION_PATTERNS.items())
        weights = [p[1]["weight"] for p in patterns]
        selected = random.choices(patterns, weights=weights)[0]
        return selected[1]
    
    def _release_enhanced_browser(self, result, browser_info):
        """Release browser back to pool with enhanced handling"""
        if isinstance(result, Failure):
            if result.check(BotDetectionError):
                logger.warning(f"Bot detection on browser with proxy {browser_info['proxy']}")
                # Don't immediately discard browser, try to recover
                try:
                    driver = browser_info["driver"]
                    # Clear cookies and reset
                    driver.delete_all_cookies()
                    # Navigate away from blocked page
                    driver.get("about:blank")
                    time.sleep(1)
                    # Reset warm-up status
                    browser_info["warmed_up"] = False
                    browser_info["request_count"] = 0
                except:
                    pass
            
            # Only recreate browser if it's really broken
            if browser_info["request_count"] > 100 or result.check(BotDetectionError):
                try:
                    browser_info["driver"].quit()
                except:
                    pass
                
                # Create replacement
                new_proxy = self.proxy_manager.get_proxy({"replacement": True})
                thread = threading.Thread(
                    target=self._create_enhanced_browser,
                    args=(random.randint(1000, 9999), new_proxy)
                )
                thread.start()
            else:
                # Return to pool for reuse
                self.browser_pool.put(browser_info)
        else:
            # Success - return to pool
            self.browser_pool.put(browser_info)
        
        return result
    
    def spider_closed(self, spider):
        """Clean up browser pool and save statistics"""
        logger.info("Closing enhanced browser pool...")
        
        # Save proxy statistics
        self.proxy_manager.save_stats()
        
        # Get final statistics
        stats = self.proxy_manager.get_stats_summary()
        logger.info(f"Final proxy statistics: {json.dumps(stats, indent=2)}")
        
        # Clean up browsers
        browsers_to_quit = []
        while not self.browser_pool.empty():
            try:
                browser_info = self.browser_pool.get_nowait()
                browsers_to_quit.append(browser_info["driver"])
            except:
                break
        
        for driver in browsers_to_quit:
            try:
                driver.quit()
            except:
                pass
        
        # Clean up processes and directories
        time.sleep(2)
        kill_chrome_processes()
        time.sleep(1)
        
        if os.path.exists(self.sessions_base_dir):
            try:
                shutil.rmtree(self.sessions_base_dir)
                logger.info(f"Cleaned up session directory: {self.sessions_base_dir}")
            except:
                logger.warning(f"Could not remove session directory: {self.sessions_base_dir}")
    
    def process_exception(self, request, exception, spider):
        """Handle exceptions with proper tracking"""
        if isinstance(exception, BotDetectionError):
            logger.warning(f"Bot detection on {request.url}")
            # Don't retry immediately
            request.meta['retry_times'] = request.meta.get('retry_times', 0) + 5
            return None
        return None 