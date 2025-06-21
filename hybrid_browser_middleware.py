import logging
import os
import queue
import threading
import time
import random
import zipfile
from pathlib import Path
import shutil
import tempfile
import datetime

from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from twisted.internet import defer
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

from middlewares import BotDetectionError

logger = logging.getLogger(__name__)

# Common, realistic user agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

class HybridBrowserMiddleware:
    """
    A fully automated middleware using premium ISP proxies and a realistic
    browser warm-up sequence to avoid bot detection.
    """
    
    # Class-level lock to prevent race conditions during browser creation
    _init_lock = threading.Lock()
    
    def __init__(self, settings):
        self.proxies = self._load_oxylabs_proxies()
        if not self.proxies:
            raise RuntimeError("Oxylabs ISP proxies not configured. Please set OXYLABS_USERNAME, OXYLABS_PASSWORD, and OXYLABS_PROXIES_PORTS environment variables.")
        
        self.browser_pool_size = len(self.proxies)
        self.browser_pool = None
        
        # Use the custom TEMP_BASE_DIR if available, otherwise use system temp
        base_dir = os.getenv('TEMP_BASE_DIR', tempfile.gettempdir())
        session_parent_dir = Path(base_dir)
        # Ensure the base directory exists
        session_parent_dir.mkdir(exist_ok=True, parents=True)

        self.sessions_dir = session_parent_dir / f"retail_scraper_sessions_{int(time.time())}"
        self.sessions_dir.mkdir(exist_ok=True)
        
        # Create a debug directory to store screenshots and HTML on failure
        self.debug_dir = Path("debug")
        self.debug_dir.mkdir(exist_ok=True)
        
        logger.info(f"Loaded {len(self.proxies)} Oxylabs ISP proxies. Session directory: {self.sessions_dir}")

    def _load_oxylabs_proxies(self):
        """Loads Oxylabs proxy details from environment variables."""
        username = os.getenv('OXYLABS_USERNAME')
        password = os.getenv('OXYLABS_PASSWORD')
        ports_str = os.getenv('OXYLABS_PROXIES_PORTS')

        if not all([username, password, ports_str]):
            logger.error("Oxylabs environment variables not fully set.")
            return []

        ports = [port.strip() for port in ports_str.split(';') if port.strip()]
        
        # Using 'isp.oxylabs.io' as the standard host for ISP proxies
        proxy_host = "isp.oxylabs.io"
        
        return [{
            "proxy_str": f"{username}:{password}@{proxy_host}:{port}",
            "host": proxy_host,
            "port": int(port),
            "user": username,
            "pass": password
        } for port in ports]

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler.settings)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def spider_opened(self, spider):
        """Initializes the browser pool, one browser per proxy."""
        self.browser_pool = queue.Queue(maxsize=self.browser_pool_size)
        logger.info(f"Creating browser pool with {self.browser_pool_size} instances, one for each Oxylabs proxy.")
        
        threads = []
        for i, proxy_info in enumerate(self.proxies):
            thread = threading.Thread(target=self._create_browser, args=(i, proxy_info))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        if self.browser_pool.empty():
            raise RuntimeError("Failed to initialize any browser instances. Check proxy credentials and Chrome installation.")
        
        logger.info(f"Browser pool ready with {self.browser_pool.qsize()} instances.")

    def _create_browser(self, index, proxy_info):
        """Creates a browser instance with essential stealth options and a dedicated proxy."""
        proxy_str = proxy_info['proxy_str']
        try:
            import undetected_chromedriver as uc
            
            options = uc.ChromeOptions()
            
            # Use true headless mode
            options.add_argument('--headless=new')
            
            # Create a unique profile for each browser instance for session isolation
            user_data_dir = self.sessions_dir / f"profile_{index}"
            options.add_argument(f"--user-data-dir={str(user_data_dir)}")

            # Robust proxying method for headless using a temporary extension
            proxy_extension = self._get_proxy_extension(proxy_info)
            options.add_extension(proxy_extension)
            
            # Essential anti-detection options
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument("--disable-blink-features=AutomationControlled")
            
            # Use a lock to prevent race conditions during uc initialization
            with self._init_lock:
                logger.info(f"Lock acquired by browser {index}. Initializing...")
                driver = uc.Chrome(options=options, version_main=None)
                logger.info(f"Browser {index} initialized. Lock released.")
            
            # Set a common user agent
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                'userAgent': random.choice(USER_AGENTS)
            })
            
            # Remove the 'webdriver' property from navigator
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            driver.set_page_load_timeout(90)
            
            browser_info = {
                "driver": driver,
                "proxy": proxy_str,
                "warmed_up": False
            }
            self.browser_pool.put(browser_info)
            logger.info(f"Browser {index} with proxy ...{proxy_str[-20:]} created successfully.")
            
        except Exception as e:
            logger.error(f"Failed to create browser {index} with proxy ...{proxy_str[-20:]}: {e}", exc_info=True)
        finally:
            # Clean up the temporary extension file
            if 'proxy_extension' in locals() and os.path.exists(proxy_extension):
                os.remove(proxy_extension)

    def _get_proxy_extension(self, proxy_info):
        """Creates a Chrome extension zip file for proxy authentication."""
        manifest_json = """
        {
            "version": "1.0.0",
            "manifest_version": 2,
            "name": "Chrome Proxy",
            "permissions": [
                "proxy",
                "tabs",
                "unlimitedStorage",
                "storage",
                "<all_urls>",
                "webRequest",
                "webRequestBlocking"
            ],
            "background": {
                "scripts": ["background.js"]
            }
        }
        """
        background_js = f"""
        var config = {{
            mode: "fixed_servers",
            rules: {{
                singleProxy: {{
                    scheme: "http",
                    host: "{proxy_info['host']}",
                    port: parseInt({proxy_info['port']})
                }},
                bypassList: ["localhost"]
            }}
        }};

        chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});

        function callbackFn(details) {{
            return {{
                authCredentials: {{
                    username: "{proxy_info['user']}",
                    password: "{proxy_info['pass']}"
                }}
            }};
        }}

        chrome.webRequest.onAuthRequired.addListener(
            callbackFn,
            {{urls: ["<all_urls>"]}},
            ['blocking']
        );
        """
        
        ext_dir = self.sessions_dir / f"proxy_ext_{proxy_info['port']}"
        ext_dir.mkdir(exist_ok=True)

        with open(ext_dir / "manifest.json", "w") as f:
            f.write(manifest_json)
        with open(ext_dir / "background.js", "w") as f:
            f.write(background_js)
        
        extension_zip_path = ext_dir.with_suffix('.zip')
        with zipfile.ZipFile(extension_zip_path, 'w') as zp:
            zp.write(ext_dir / "manifest.json", "manifest.json")
            zp.write(ext_dir / "background.js", "background.js")
            
        shutil.rmtree(ext_dir) # Clean up the temp folder

        return str(extension_zip_path)

    def process_request(self, request, spider):
        """Processes a request using a browser from the pool."""
        if not request.meta.get("use_undetected_browser"):
            return None
        
        try:
            browser_info = self.browser_pool.get(timeout=120)
            
            # Defer the execution to a thread
            deferred = defer.maybeDeferred(self._execute_request, request, browser_info)
            # Add a callback to return the browser to the pool
            deferred.addBoth(self._return_browser, browser_info)
            
            return deferred
            
        except queue.Empty:
            raise IgnoreRequest("No available browsers in the pool. Increase pool size or check for errors.")

    def _execute_request(self, request, browser_info):
        """Executes the request, performing a warm-up if necessary."""
        driver = browser_info["driver"]
        
        try:
            # Warm up the browser on its first run
            if not browser_info["warmed_up"]:
                self._warm_up_browser(driver, browser_info["proxy"])
                browser_info["warmed_up"] = True

                # Add a small, human-like interaction on the homepage after warm-up
                logger.info("Performing post-warmup interaction on homepage...")
                time.sleep(random.uniform(1.5, 2.5))
                driver.execute_script("window.scrollBy(0, 250);")
                time.sleep(random.uniform(0.5, 1.5))

            logger.info(f"Processing {request.url} with proxy ...{browser_info['proxy'][-20:]}")
            driver.get(request.url)
            time.sleep(random.uniform(3.0, 5.0)) # Wait for page to settle

            if self._check_bot_detection(driver):
                # Save debug info before raising the error
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                proxy_safe_name = browser_info['proxy'].split('@')[-1].replace(':', '_')
                
                screenshot_path = self.debug_dir / f"bot_detection_{proxy_safe_name}_{timestamp}.png"
                html_path = self.debug_dir / f"bot_detection_{proxy_safe_name}_{timestamp}.html"
                
                driver.save_screenshot(str(screenshot_path))
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                
                logger.warning(f"BOT DETECTION on {request.url} with proxy ...{browser_info['proxy'][-20:]}")
                logger.warning(f"  -> Page Title: {driver.title}")
                logger.warning(f"  -> Current URL: {driver.current_url}")
                logger.warning(f"  -> Debug screenshot saved to: {screenshot_path}")
                logger.warning(f"  -> Debug HTML saved to: {html_path}")
                
                # Attempt to solve the CAPTCHA before giving up
                if "robot or human" in driver.title.lower():
                    logger.info("Attempting to solve the 'Press and Hold' CAPTCHA...")
                    if self._solve_press_and_hold_captcha(driver):
                        logger.info("CAPTCHA solved successfully! Retrying original request.")
                        # After solving, we can re-try the request. Scrapy's retry middleware
                        # will handle this if we raise the original error.
                        # For simplicity here, we will just continue with the now-unlocked page.
                        # Re-check the page to ensure we are no longer blocked.
                        if not self._check_bot_detection(driver):
                             logger.info("Page is unlocked. Proceeding with content extraction.")
                        else:
                             logger.error("Failed to solve CAPTCHA, page is still blocked.")
                             raise BotDetectionError("Failed to solve 'Press and Hold' CAPTCHA.")
                    else:
                        logger.error("CAPTCHA solver failed.")
                        raise BotDetectionError("Bot detection page encountered and solver failed.")
                else:
                    raise BotDetectionError("Bot detection page encountered.")

            # Perform a small scroll to mimic user behavior and trigger lazy-loaded content
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.2);")
            time.sleep(random.uniform(1.0, 2.0))

            return HtmlResponse(
                request.url,
                body=driver.page_source.encode('utf-8'),
                encoding='utf-8',
                request=request,
                status=200
            )
            
        except Exception as e:
            logger.error(f"Request failed for {request.url} with proxy ...{browser_info['proxy'][-10:]}: {e}")
            # Re-raise to be handled by Scrapy's retry mechanism
            raise

    def _warm_up_browser(self, driver, proxy):
        """Warms up the browser by mimicking a natural user journey."""
        logger.info(f"Warming up browser with proxy ...{proxy[-20:]}")
        
        try:
            # 1. Start at a search engine
            driver.get("https://www.google.com")
            
            # Handle cookie consent pop-ups that can block clicks
            try:
                reject_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Reject all') or contains(., 'Rifiuta tutto')]"))
                )
                logger.info("Cookie consent banner found. Clicking 'Reject all'.")
                reject_button.click()
                time.sleep(random.uniform(1, 2))
            except:
                logger.info("No cookie consent banner found, or it was not clickable. Proceeding.")

            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.NAME, "q")))
            
            # 2. Search for "walmart"
            search_box = driver.find_element(By.NAME, "q")
            search_term = "walmart official site"
            for char in search_term:
                search_box.send_keys(char)
                time.sleep(random.uniform(0.05, 0.15))
            search_box.submit()
            
            # 3. Find and click the official Walmart link
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='www.walmart.com']")))
            
            # Find a link that is highly likely to be the main site
            walmart_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='www.walmart.com']")
            target_link = None
            for link in walmart_links:
                href = link.get_attribute('href')
                if href and 'walmart.com' in href and 'google.com' not in href and 'accounts.google.com' not in href:
                    target_link = link
                    break
            
            if not target_link:
                raise RuntimeError("Could not find a valid Walmart link in search results.")

            # Use JavaScript click as a fallback to avoid interception
            driver.execute_script("arguments[0].click();", target_link)

            # 4. Wait for the Walmart page to load
            WebDriverWait(driver, 20).until(lambda d: d.current_url.startswith("https://www.walmart.com"))
            time.sleep(random.uniform(3, 5))
            
            logger.info(f"Warm-up successful for proxy ...{proxy[-20:]}")
            
        except Exception as e:
            logger.error(f"Warm-up failed for proxy ...{proxy[-20:]}. The browser will proceed without warm-up. Error: {e}")

    def _check_bot_detection(self, driver):
        """Checks for common bot detection indicators."""
        title = driver.title.lower()
        source = driver.page_source.lower()
        
        indicators = ["robot or human", "are you a robot", "access denied", "please verify", "security check", "challenge"]
        for indicator in indicators:
            if indicator in title or indicator in source:
                # Add a check to ensure we are not on a valid product page that contains the word "challenge"
                if indicator == "challenge" and "/ip/" in driver.current_url:
                    continue
                return True
        return False

    def _solve_press_and_hold_captcha(self, driver):
        """Attempts to solve the 'Press and Hold' CAPTCHA with more human-like interaction."""
        try:
            # Simulate a user assessing the page before acting
            logger.info("Human-like pre-interaction: moving mouse and clicking body...")
            actions = ActionChains(driver)
            actions.move_by_offset(random.randint(50, 150), random.randint(50, 150)).perform()
            time.sleep(random.uniform(0.5, 1.0))
            actions.move_by_offset(random.randint(-50, 50), random.randint(-50, 50)).perform()
            driver.find_element(By.TAG_NAME, "body").click()
            time.sleep(random.uniform(1.0, 1.5))

            # Step 1: Wait for the iframe to exist in the DOM first.
            logger.info("Waiting for CAPTCHA iframe to be present in DOM...")
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#px-captcha iframe"))
            )
            
            # Give the page's scripts a moment to execute
            time.sleep(random.uniform(1, 2))

            # Step 2: Now, wait for the frame to be available and switch to it
            logger.info("Waiting for CAPTCHA iframe to be available...")
            WebDriverWait(driver, 20).until(
                EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "#px-captcha iframe"))
            )
            
            logger.info("Switched to iframe. Searching for the hold button...")
            # Use a more robust, structural selector that doesn't depend on language
            hold_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div[role='button']"))
            )
            
            # Perform the press and hold action with realistic, randomized timing
            hold_duration = random.uniform(8.5, 12.5)
            logger.info(f"Pressing and holding button for {hold_duration:.2f} seconds...")
            
            # Reset actions and perform the hold
            actions = ActionChains(driver)
            # Add small random pauses to make the action less robotic
            actions.move_to_element(hold_button).pause(random.uniform(0.1, 0.4)).click_and_hold().pause(hold_duration).release().perform()

            logger.info("Button released.")
            
            # Switch back to the main content
            driver.switch_to.default_content()
            
            # Wait for a moment to see if the page reloads or changes
            logger.info("Waiting for page to unlock...")
            time.sleep(5)
            
            return True
            
        except Exception as e:
            logger.error(f"Error occurred while trying to solve CAPTCHA: {e}")
            # Ensure we switch back to default content on failure
            try:
                driver.switch_to.default_content()
            except:
                pass
            return False

    def _return_browser(self, result, browser_info):
        """Returns the browser to the pool for reuse."""
        self.browser_pool.put(browser_info)
        return result

    def spider_closed(self, spider):
        """Cleans up all browser instances."""
        logger.info("Closing all browsers in the pool...")
        while not self.browser_pool.empty():
            try:
                browser_info = self.browser_pool.get_nowait()
                browser_info["driver"].quit()
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error quitting browser: {e}")
        
        # Clean up the session directory
        try:
            shutil.rmtree(self.sessions_dir)
            logger.info(f"Cleaned up session directory: {self.sessions_dir}")
        except Exception as e:
            logger.warning(f"Could not remove session directory {self.sessions_dir}: {e}") 