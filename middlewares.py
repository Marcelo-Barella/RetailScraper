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

from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from twisted.internet import defer
from twisted.python.failure import Failure
from fake_useragent import UserAgent

from helpers.helpers import ProxyManager
from helpers.config import TEMP_BROWSER_SESSIONS_POOL_DIR

logger = logging.getLogger(__name__)

class BotDetectionError(Exception):
    """Custom exception for when a bot detection page is encountered."""
    pass

# Define project root to build absolute paths for our managed temp dir
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def kill_chrome_processes():
    """Kill all Chrome and chromedriver processes to release file locks."""
    try:
        # Try using psutil first for more reliable process killing
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if proc.info['name'] and ('chrome' in proc.info['name'].lower() or 'chromedriver' in proc.info['name'].lower()):
                    proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except:
        # Fallback to subprocess if psutil fails
        if os.name == 'nt':  # Windows
            try:
                subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe'], capture_output=True, check=False)
                subprocess.run(['taskkill', '/F', '/IM', 'chromedriver.exe'], capture_output=True, check=False)
            except:
                pass


class UnifiedProxyBrowserMiddleware:
    """
    Manages a pool of persistent browser instances to handle browser-based
    requests efficiently, solving memory and disk leaks. It cleans up old
    browser profiles on startup to prevent accumulation from crashed runs.
    """
    MAX_BROWSER_RETRIES = 10
    _init_lock = threading.Lock()  # Class-level lock for browser initialization

    def __init__(self, proxy_manager, settings):
        self.proxy_manager = proxy_manager
        self.max_proxy_failures = settings.getint('MAX_PROXY_FAILURES', 3)
        self.browser_pool_size = settings.getint('BROWSER_POOL_SIZE', settings.getint('CONCURRENT_REQUESTS', 10))
        self.browser_pool = None
        self.user_data_dirs = []

        # Use a unique directory name for each run to avoid conflicts
        timestamp = int(time.time() * 1000)  # Millisecond precision
        self.sessions_base_dir = os.path.join(
            os.path.dirname(TEMP_BROWSER_SESSIONS_POOL_DIR), 
            f'browser_sessions_pool_{timestamp}'
        )
        
        # Clean up any old directories from previous runs
        base_dir = os.path.dirname(TEMP_BROWSER_SESSIONS_POOL_DIR)
        if os.path.exists(base_dir):
            for item in os.listdir(base_dir):
                if item.startswith('browser_sessions_pool_'):
                    old_dir = os.path.join(base_dir, item)
                    try:
                        # Try to remove old directories
                        shutil.rmtree(old_dir)
                        logger.info(f"Cleaned up old session directory: {old_dir}")
                    except Exception as e:
                        # If we can't remove it, it's probably from a running instance
                        logger.debug(f"Could not remove {old_dir}: {e}")
        
        # Create our new directory
        os.makedirs(self.sessions_base_dir, exist_ok=True)
        logger.info(f"Browser pool using session directory: {self.sessions_base_dir}")
        logger.info(f"Browser pool size set to: {self.browser_pool_size}")

    @classmethod
    def from_crawler(cls, crawler):
        proxy_manager = ProxyManager()
        middleware = cls(proxy_manager, crawler.settings)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def spider_opened(self, spider):
        """Initialize the browser pool when the spider starts."""
        self.browser_pool = queue.Queue(maxsize=self.browser_pool_size)
        logger.info(f"Creating browser pool with {self.browser_pool_size} instances...")

        threads = []
        for i in range(self.browser_pool_size):
            # Each browser gets one proxy for its lifetime for stability.
            proxy = self.proxy_manager.get_proxy()
            thread = threading.Thread(target=self._create_and_add_browser_to_pool, args=(i, proxy))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        if self.browser_pool.empty():
            raise RuntimeError("Failed to initialize any browser instances in the pool.")
        logger.info(f"Browser pool initialized with {self.browser_pool.qsize()} instances.")

    def _create_and_add_browser_to_pool(self, index, proxy):
        """Worker function to create a browser and add it to the pool."""
        try:
            user_data_dir = tempfile.mkdtemp(prefix=f"uc_browser_{index}_", dir=self.sessions_base_dir)
            self.user_data_dirs.append(user_data_dir)
            
            driver = self._create_browser_instance(user_data_dir=user_data_dir, proxy=proxy)
            if driver:
                browser_info = {
                    "driver": driver,
                    "proxy": proxy,
                    "user_data_dir": user_data_dir,
                    "warmed_up": False,
                }
                self.browser_pool.put(browser_info)
            else:
                logger.error(f"Failed to create browser instance {index+1}/{self.browser_pool_size}")
        except Exception as e:
            logger.error(f"Error creating browser instance in pool: {e}", exc_info=True)

    def spider_closed(self, spider):
        """Clean up the browser pool and session directories when the spider finishes."""
        logger.info("Closing all browser instances in the pool...")
        
        # First, quit all browser instances
        browsers_to_quit = []
        while not self.browser_pool.empty():
            try:
                browser_info = self.browser_pool.get_nowait()
                browsers_to_quit.append(browser_info["driver"])
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error getting browser from pool: {e}")
        
        # Quit browsers
        for driver in browsers_to_quit:
            try:
                driver.quit()
            except Exception as e:
                logger.debug(f"Error quitting browser: {e}")
        
        # Give browsers time to fully close
        time.sleep(2)
        
        # Kill any remaining Chrome processes
        kill_chrome_processes()
        time.sleep(1)
        
        # Now try to clean up the directory
        if os.path.exists(self.sessions_base_dir):
            for attempt in range(3):
                try:
                    shutil.rmtree(self.sessions_base_dir)
                    logger.info(f"Successfully removed browser sessions directory: {self.sessions_base_dir}")
                    break
                except Exception as e:
                    if attempt < 2:
                        logger.warning(f"Failed to remove sessions directory (attempt {attempt + 1}/3): {e}")
                        time.sleep(2)
                        # Try killing Chrome processes again
                        kill_chrome_processes()
                        time.sleep(1)
                    else:
                        # Final attempt - just log the error and move on
                        logger.warning(f"Could not remove sessions directory {self.sessions_base_dir}: {e}")
                        logger.warning("Directory will remain for manual cleanup or next run")

    def process_request(self, request, spider):
        if not request.meta.get("use_undetected_browser"):
            return None

        try:
            browser_info = self.browser_pool.get(timeout=300)
            request.meta['proxy'] = browser_info["proxy"]
        except queue.Empty:
            raise IgnoreRequest("Timed out waiting for an available browser from the pool.")

        deferred = defer.maybeDeferred(self._execute_browser_request, request, browser_info)
        deferred.addBoth(self._release_browser, browser_info)
        return deferred

    def process_exception(self, request, exception, spider):
        """Handle exceptions that occur during request processing."""
        if isinstance(exception, BotDetectionError):
            # Log the bot detection
            logger.warning(f"Bot detection error on {request.url}, will retry with different proxy")
            
            # Don't set dont_retry, let RetryMiddleware handle it
            request.meta.pop('dont_retry', None)
            
            # The RetryMiddleware will retry this request
            return None
        
        # For other exceptions, let them propagate
        return None

    def _release_browser(self, result, browser_info):
        """
        Callback/errback to return a browser to the pool, or replace it if it failed.
        """
        if isinstance(result, Failure):
            # Check if it's a bot detection failure
            if result.check(BotDetectionError):
                logger.warning(f"Browser with proxy {browser_info['proxy']} hit bot detection. Discarding and creating a replacement.")
                # Clear cookies to appear as a new visitor
                try:
                    browser_info["driver"].delete_all_cookies()
                    browser_info["warmed_up"] = False  # Force re-warming
                except:
                    pass
            else:
                logger.warning(f"Browser with proxy {browser_info['proxy']} failed with: {result.getErrorMessage()}")
            
            try:
                browser_info["driver"].quit()
            except Exception as e:
                logger.error(f"Error quitting a failed browser instance: {e}")
            
            # Start a new thread to create a replacement browser without blocking
            # Use a random index to avoid potential temp folder name collisions
            replacement_index = random.randint(1000, 9999)
            thread = threading.Thread(target=self._create_and_add_browser_to_pool, args=(replacement_index, self.proxy_manager.get_proxy()))
            thread.start()
        else:
            # Request was successful, return browser to the pool
            self.browser_pool.put(browser_info)
        
        return result # Important: Pass the result/failure along the chain

    def _execute_browser_request(self, request, browser_info):
        """Executes a browser request using a driver from the pool."""
        driver = browser_info["driver"]
        proxy = browser_info["proxy"]
        logger.info(f"[Browser Pool] Using proxy {proxy} for {request.url}")

        try:
            from selenium.webdriver.common.action_chains import ActionChains
            # Warm-up phase: Visit the homepage once per browser instance
            if not browser_info["warmed_up"]:
                logger.info(f"Warming up browser for {request.url}")
                driver.get("https://www.walmart.com/")
                time.sleep(random.uniform(3, 6))  # Longer warm-up delay
                
                # Perform some human-like actions on the homepage
                try:
                    # Scroll a bit
                    driver.execute_script("window.scrollBy(0, 300);")
                    time.sleep(random.uniform(1, 2))
                    driver.execute_script("window.scrollBy(0, -150);")
                    time.sleep(random.uniform(0.5, 1))
                except:
                    pass
                
                browser_info["warmed_up"] = True
            
            # Add random delay before navigation to mimic human browsing
            time.sleep(random.uniform(0.5, 2))
            
            driver.get(request.url)
            
            # Variable wait time based on page complexity
            initial_wait = random.uniform(3, 6)
            time.sleep(initial_wait)
            
            # Wait for page to be interactive
            try:
                driver.execute_script("return document.readyState")
                # Additional wait if page is still loading
                for _ in range(5):
                    if driver.execute_script("return document.readyState") == "complete":
                        break
                    time.sleep(0.5)
            except:
                pass

            # --- Advanced Human-like Interaction ---
            # 1. Random mouse movements
            actions = ActionChains(driver)
            for _ in range(random.randint(3, 7)):
                x_offset = random.randint(100, driver.execute_script("return window.innerWidth;") - 100)
                y_offset = random.randint(100, driver.execute_script("return window.innerHeight;") - 100)
                actions.move_by_offset(x_offset, y_offset).pause(random.uniform(0.2, 0.8)).perform()
                actions.reset_actions() # Reset for next move

            # 2. Realistic scrolling
            for _ in range(random.randint(1, 3)):
                scroll_height = driver.execute_script("return document.body.scrollHeight")
                scroll_to = scroll_height * random.uniform(0.2, 0.4)
                driver.execute_script(f"window.scrollBy(0, {scroll_to});")
                time.sleep(random.uniform(0.8, 1.5))

            # Check for bot detection page
            if "robot or human?" in driver.page_source.lower() or "are you a robot" in driver.page_source.lower():
                logger.warning(f"Bot detection page encountered with proxy {proxy} on {request.url}")
                self.proxy_manager.record_failure(proxy)
                raise BotDetectionError(f"Bot detection page encountered with proxy {proxy}")
            
            # Check if we were redirected to a blocked URL
            if "/blocked" in driver.current_url:
                logger.warning(f"Redirected to blocked URL with proxy {proxy} on {request.url}")
                self.proxy_manager.record_failure(proxy)
                raise BotDetectionError(f"Redirected to blocked URL with proxy {proxy}")

            # Record successful proxy usage
            if hasattr(self.proxy_manager, 'record_success'):
                self.proxy_manager.record_success(proxy)
            
            return HtmlResponse(
                request.url,
                body=driver.page_source.encode('utf-8'),
                encoding='utf-8',
                request=request,
                status=200
            )
        except BotDetectionError:
            # Re-raise BotDetectionError to ensure it's handled by retry middleware
            raise
        except Exception as e:
            logger.error(f"[Browser Pool] Failed request with proxy {proxy} for {request.url}: {e}")
            self.proxy_manager.record_failure(proxy)
            # Re-raise the exception to be handled by Scrapy's retry mechanisms
            raise

    def _create_browser_instance(self, user_data_dir, proxy):
        """Helper to create a single uc.Chrome instance with the specified proxy."""
        try:
            import undetected_chromedriver as uc
            from selenium.webdriver.common.proxy import Proxy, ProxyType
            
            options = uc.ChromeOptions()
            options.add_argument('--headless=new')

            # --- Advanced Fingerprinting Evasion ---
            user_agent = random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ])
            options.add_argument(f'--user-agent={user_agent}')
            
            # Make other headers consistent with a modern Chrome browser
            options.add_argument('--accept-lang=en-US,en;q=0.9')
            options.add_argument(f'--window-size={random.choice(["1920,1080", "1536,864", "1440,900"])}')
            
            # Anti-detection measures using well-supported arguments
            options.add_argument("--disable-automation")
            options.add_argument("--disable-web-security")
            options.add_argument("--allow-running-insecure-content")
            
            # Standard flags
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-gpu')
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--ignore-certificate-errors")

            if proxy:
                proxy_address = proxy.split('://', 1)[-1]
                
                selenium_proxy = Proxy()
                selenium_proxy.proxy_type = ProxyType.MANUAL
                selenium_proxy.http_proxy = proxy_address
                selenium_proxy.ssl_proxy = proxy_address

                options.proxy = selenium_proxy
            
            logger.info(f"Creating browser with User-Agent: {user_agent}")
            
            with self._init_lock:
                driver = uc.Chrome(options=options, user_data_dir=user_data_dir)
            
            # Enhanced JavaScript execution to hide automation
            driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                window.chrome = {runtime: {}};
                Object.defineProperty(navigator, 'permissions', {
                    get: () => ({
                        query: () => Promise.resolve({state: 'granted'})
                    })
                });
            """)
            
            driver.set_page_load_timeout(90) # Increased timeout
            return driver

        except Exception as e:
            logger.error(f"Failed to create browser instance with proxy {proxy}: {e}", exc_info=True)
            return None