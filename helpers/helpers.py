import os
import time
import random
import json
import xml.etree.ElementTree as ET
from typing import List, Optional, Dict, Any
from datetime import datetime
import subprocess
import re

import requests
from bs4 import BeautifulSoup
import threading
import tempfile
import shutil

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.proxy import Proxy, ProxyType
except ImportError:
    uc = None

# Import temp directory configuration
from helpers.config import TEMP_BASE_DIR, TEMP_BROWSER_SESSIONS_DIR, TEMP_BROWSER_SESSIONS_POOL_DIR

# Define project root to build absolute paths for local temp directories
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Global lock to prevent race conditions during concurrent Chrome initialization
chrome_init_lock = threading.Lock()


# A diverse list of common, recent user agents to randomize (updated December 2024)
# NOTE: Update this list periodically with latest browser versions to avoid detection
USER_AGENTS = [
    # Windows - Latest Chrome versions
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.166 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.70 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6723.116 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.166 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.166 Safari/537.36 Edg/131.0.2903.112",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    # macOS - Latest versions
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.166 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.85 Safari/537.36",
    # Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.166 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.70 Safari/537.36",
    # Android - Latest Chrome versions
    "Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.166 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.70 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.166 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.85 Mobile Safari/537.36",
    # iOS - Latest versions
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/134.0.6998.166 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1"
]

# Common screen resolutions to randomize
SCREEN_RESOLUTIONS = [
    "1920,1080",
    "1366,768",
    "1536,864",
    "1440,900",
    "1280,720",
]


DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.166 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Sec-Ch-Ua": '"Google Chrome";v="134", "Chromium";v="134", "Not=A?Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Dnt": "1",
}


class ProxyManager:
    """Loads and manages a list of proxies from a JSON file with quality-based prioritization."""

    def __init__(self, proxy_file: str = None):
        if proxy_file is None:
            self.proxy_file = "helpers/proxies.json"
        else:
            self.proxy_file = proxy_file
            
        self.all_proxies = []  # Store full proxy objects
        self.proxies = []  # Store proxy URLs for compatibility
        self.proxy_by_url = {}  # Map URL to full proxy object
        self.proxy_scores = {}  # Track dynamic scores
        self.failed_proxies = set()
        self.lock = threading.Lock()
        
        self._load_and_sort_proxies()
        self._proxy_iterator = iter(self.proxies) if self.proxies else iter([])

        if self.proxies:
            print(f"ProxyManager: Loaded {len(self.proxies)} proxies.")
            self._print_proxy_summary()
        else:
            print("ProxyManager: No proxies loaded. Running without proxies.")
            print("ProxyManager: Add proxies to helpers/proxies.json for proxy support.")

    def _load_and_sort_proxies(self) -> None:
        """Load proxies from JSON file and sort by quality score."""
        try:
            with open(self.proxy_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            proxy_objects = []
            if isinstance(data, list):
                proxy_objects = data
            elif isinstance(data, dict):
                proxy_objects = data.get("proxies", [])

            # Process and categorize proxies
            residential_proxies = []
            high_quality_proxies = []
            medium_quality_proxies = []
            low_quality_proxies = []
            socks_proxies = []

            for p in proxy_objects:
                # Extract proxy information
                ip = p.get("ip") or p.get("host") or None
                port = p.get("port")
                protocol = p.get("protocol", "http").lower()
                explicit_url = p.get("proxy")
                quality_score = p.get("quality_score", 0)
                is_residential = p.get("is_residential", False)
                proxy_type = p.get("type", "datacenter")

                # Skip SOCKS proxies as they don't work with Selenium WebDriver
                if protocol in ["socks", "socks4", "socks5"]:
                    socks_proxies.append(p)
                    continue

                # Construct proxy URL
                if explicit_url:
                    proxy_url = explicit_url
                elif ip and port:
                    if protocol == "https":
                        proxy_url = f"https://{ip}:{port}"
                    else:
                        proxy_url = f"http://{ip}:{port}"
                else:
                    continue

                # Store full proxy object
                p["url"] = proxy_url
                self.all_proxies.append(p)
                self.proxy_by_url[proxy_url] = p

                # Categorize by quality
                if is_residential or proxy_type == "residential":
                    residential_proxies.append(proxy_url)
                elif quality_score >= 10:
                    high_quality_proxies.append(proxy_url)
                elif quality_score >= 5:
                    medium_quality_proxies.append(proxy_url)
                else:
                    low_quality_proxies.append(proxy_url)

            # Build final proxy list in priority order:
            # 1. Residential proxies (highest priority)
            # 2. High quality datacenter proxies
            # 3. Medium quality datacenter proxies
            # 4. Low quality datacenter proxies
            self.proxies = (
                residential_proxies + 
                high_quality_proxies + 
                medium_quality_proxies + 
                low_quality_proxies
            )
            
            # Initialize dynamic scores
            for url in self.proxies:
                self.proxy_scores[url] = self.proxy_by_url[url].get("quality_score", 0)
            
            if socks_proxies:
                print(f"ProxyManager: Skipped {len(socks_proxies)} SOCKS proxies (not supported with Selenium)")
                
        except FileNotFoundError:
            self.proxies = []
            self.all_proxies = []
        except json.JSONDecodeError:
            print(f"Warning: Could not decode JSON from {self.proxy_file}.")
            self.proxies = []
            self.all_proxies = []

    def _print_proxy_summary(self):
        """Print a summary of loaded proxies."""
        residential_count = len([p for p in self.all_proxies if p.get("is_residential", False)])
        datacenter_count = len([p for p in self.all_proxies if not p.get("is_residential", False)])
        
        print(f"ProxyManager: Proxy Summary:")
        print(f"  - Residential: {residential_count}")
        print(f"  - Datacenter: {datacenter_count}")
        
        if residential_count > 0:
            print(f"  ✨ Found residential proxies! These will be prioritized.")

    def get_proxy(self) -> Optional[str]:
        """Return the next available proxy, prioritizing by quality score."""
        with self.lock:
            # Filter out failed proxies and sort by current score
            available_proxies = [
                (url, self.proxy_scores.get(url, 0)) 
                for url in self.proxies 
                if url not in self.failed_proxies
            ]
            
            if not available_proxies:
                # All proxies have failed, reset failed proxies
                print("ProxyManager: All proxies have failed. Resetting failed proxy list.")
                self.failed_proxies.clear()
                # Reset scores for all proxies
                for url in self.proxies:
                    original_score = self.proxy_by_url[url].get("quality_score", 0)
                    self.proxy_scores[url] = original_score
                available_proxies = [(url, self.proxy_scores[url]) for url in self.proxies]
            
            if not available_proxies:
                return None
            
            # Sort by score (highest first) and return the best one
            available_proxies.sort(key=lambda x: x[1], reverse=True)
            best_proxy_url = available_proxies[0][0]
            
            # Slightly decrease score to rotate through proxies
            self.proxy_scores[best_proxy_url] = max(0, self.proxy_scores[best_proxy_url] - 0.1)
            
            return best_proxy_url

    def record_failure(self, proxy: str):
        """Records a proxy as having failed and decreases its score."""
        with self.lock:
            self.failed_proxies.add(proxy)
            # Significantly decrease score for failed proxy
            self.proxy_scores[proxy] = max(-10, self.proxy_scores.get(proxy, 0) - 5)
            
            proxy_info = self.proxy_by_url.get(proxy, {})
            proxy_type = "Residential" if proxy_info.get("is_residential", False) else "Datacenter"
            print(f"Proxy failure recorded for: {proxy} ({proxy_type}). Total failed: {len(self.failed_proxies)}")

    def record_success(self, proxy: str):
        """Records a proxy as having succeeded and increases its score."""
        with self.lock:
            # Remove from failed set if it was there
            self.failed_proxies.discard(proxy)
            # Increase score for successful proxy
            self.proxy_scores[proxy] = self.proxy_scores.get(proxy, 0) + 2
            
            proxy_info = self.proxy_by_url.get(proxy, {})
            proxy_type = "Residential" if proxy_info.get("is_residential", False) else "Datacenter"
            print(f"Proxy success recorded for: {proxy} ({proxy_type}). New score: {self.proxy_scores[proxy]:.1f}")

    def get_random_proxy(self) -> Optional[str]:
        """Return a single random proxy URL string, weighted by quality."""
        available_proxies = [p for p in self.proxies if p not in self.failed_proxies]
        if not available_proxies:
            return None
        
        # Weight selection by quality score
        weights = [max(1, self.proxy_scores.get(p, 1)) for p in available_proxies]
        total_weight = sum(weights)
        
        if total_weight == 0:
            return random.choice(available_proxies)
        
        # Weighted random selection
        r = random.uniform(0, total_weight)
        upto = 0
        for proxy, weight in zip(available_proxies, weights):
            if upto + weight >= r:
                return proxy
            upto += weight
        
        return available_proxies[-1]

    def get_random_proxy_dict(self) -> Optional[Dict[str, str]]:
        """Return a random proxy in a requests-compatible format."""
        proxy = self.get_random_proxy()
        if proxy and proxy not in self.failed_proxies:
            return {"http": proxy, "https": proxy}
        return None

    def is_available(self) -> bool:
        """Check if any proxies are available."""
        return bool(self.proxies)
        
    def get_stats(self) -> Dict[str, Any]:
        """Get detailed statistics about proxy usage."""
        with self.lock:
            residential_proxies = [p for p in self.all_proxies if p.get("is_residential", False)]
            working_residential = [
                p for p in residential_proxies 
                if p.get("url") not in self.failed_proxies
            ]
            
            return {
                "total_proxies": len(self.proxies),
                "failed_proxies": len(self.failed_proxies),
                "working_proxies": len(self.proxies) - len(self.failed_proxies),
                "residential_total": len(residential_proxies),
                "residential_working": len(working_residential),
                "datacenter_total": len(self.all_proxies) - len(residential_proxies),
                "top_proxies": [
                    {
                        "url": url,
                        "score": score,
                        "type": "Residential" if self.proxy_by_url.get(url, {}).get("is_residential", False) else "Datacenter",
                        "failed": url in self.failed_proxies
                    }
                    for url, score in sorted(self.proxy_scores.items(), key=lambda x: x[1], reverse=True)[:5]
                ]
            }


def ensure_dir(directory_path: str):
    """Ensures that a directory exists, creating it if it does not."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def make_request_with_retries(
    method: str,
    url: str,
    retries: int = 5,
    backoff_factor: float = 0.5,
    proxy_manager: Optional[ProxyManager] = None,
    **kwargs,
) -> Optional[requests.Response]:
    """
    Make an HTTP request with exponential backoff.

    Parameters
    ----------
    method : str
        HTTP method (e.g., 'GET', 'HEAD', 'POST').
    url : str
        The URL to request.
    retries : int, optional
        Number of retries. Defaults to 5.
    backoff_factor : float, optional
        Factor for calculating sleep time. Defaults to 0.5.
    proxy_manager : Optional[ProxyManager], optional
        A ProxyManager instance to get proxies from. Defaults to None.
    **kwargs
        Additional arguments to pass to requests.request().

    Returns
    -------
    Optional[requests.Response]
        The response object or None if all retries fail.
    """
    session = requests.Session()
    headers = DEFAULT_HEADERS.copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)
    if 'headers' in kwargs:
        headers.update(kwargs['headers'])
    kwargs['headers'] = headers

    for i in range(retries):
        request_kwargs = kwargs.copy()
        if proxy_manager and proxy_manager.is_available():
            request_kwargs["proxies"] = proxy_manager.get_random_proxy_dict()

        try:
            response = session.request(method, url, **request_kwargs)
            if 500 <= response.status_code < 600 or response.status_code == 429:
                response.raise_for_status()
            return response
        except requests.RequestException as e:
            if i < retries - 1:
                sleep_time = backoff_factor * (2 ** i)
                time.sleep(sleep_time)
            else:
                return None


def fetch_content_browser(
    url: str,
    wait_seconds: int = 15,
    headless: bool = True,
    proxy: Optional[str] = None
) -> Optional[str]:
    if uc is None:
        print("undetected_chromedriver is not installed. Cannot fetch via browser.")
        return None

    print(f"Launching {'headless' if headless else 'headed'} browser to fetch: {url}")
    
    # Ensure a local temp directory exists for browser sessions
    local_temp_dir = TEMP_BROWSER_SESSIONS_DIR
    ensure_dir(local_temp_dir)

    # Create a unique temporary directory for the browser's user data.
    # This helps in isolating browser sessions and avoiding conflicts.
    with tempfile.TemporaryDirectory(dir=local_temp_dir) as temp_user_data_dir:
        options = uc.ChromeOptions()
        if headless:
            options.add_argument('--headless=new')
        
        # --- Start Advanced Anti-Bot Configuration ---

        # 1. Randomize User-Agent and Resolution from a curated, realistic list
        user_agent = random.choice(USER_AGENTS)
        resolution = random.choice(SCREEN_RESOLUTIONS)
        options.add_argument(f'--user-agent={user_agent}')
        options.add_argument(f'--window-size={resolution}')
        
        # 2. Set consistent language headers
        options.add_argument('--lang=en-US')
        options.add_argument('--accept-lang=en-US,en;q=0.9')

        if proxy:
            proxy_address = proxy.split('://', 1)[-1]
            
            selenium_proxy = Proxy()
            selenium_proxy.proxy_type = ProxyType.MANUAL
            selenium_proxy.http_proxy = proxy_address
            selenium_proxy.ssl_proxy = proxy_address

            options.proxy = selenium_proxy

        # 3. Standard anti-bot flags to disguise automation
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-gpu') # Often used in headless environments

        # --- End Advanced Anti-Bot Configuration ---

        driver = None
        try:
            with chrome_init_lock:
                driver = uc.Chrome(options=options, user_data_dir=temp_user_data_dir)

            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.set_page_load_timeout(60)

            try:
                driver.get(url)
            except Exception as e:
                print(f"Error during driver.get({url}): {e}")
                return None

            # 5. Add a more convincing, multi-step human interaction
            time.sleep(random.uniform(1.5, 2.5))
            
            # Scroll down in a few small, random steps
            for _ in range(random.randint(2, 4)):
                scroll_height = driver.execute_script("return document.body.scrollHeight")
                scroll_to = scroll_height * random.uniform(0.1, 0.3)
                driver.execute_script(f"window.scrollBy(0, {scroll_to});")
                time.sleep(random.uniform(0.5, 1.0))

            page_content = driver.page_source
            
            error_signatures = [
                "robot or human?", "this site can't be reached", "connection timed out",
                "proxy connection failed", "err_proxy_connection_failed", "access denied",
                "enable javascript", "não é possível acessar esse site"
            ]
            
            page_text_lower = (driver.title + " " + driver.execute_script("return document.body.innerText || ''")).lower()

            for signature in error_signatures:
                if signature in page_text_lower:
                    print(f"Error: Detected blocking page. Signature: '{signature}'. Title: '{driver.title}'. URL: {url}")
                    ensure_dir("debug")
                    with open(os.path.join("debug", "blocked_page_dump.html"), "w", encoding="utf-8") as f:
                        f.write(page_content)
                    print("The blocked page HTML has been saved to 'debug\\blocked_page_dump.html' for inspection.")
                    return None

            return page_content

        except Exception as e:
            print(f"An unexpected error occurred in fetch_content_browser: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            if driver:
                driver.quit()


def extract_next_data(html_content: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html_content, 'html.parser')
    next_data_script = soup.find('script', id='__NEXT_DATA__')
    if next_data_script:
        try:
            return json.loads(next_data_script.string)
        except json.JSONDecodeError:
            return None
    return None


def parse_xml_loc_tags(xml_content: str) -> List[str]:
    urls = []
    try:
        root = ET.fromstring(xml_content)
        for url_element in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc'):
            if url_element.text:
                urls.append(url_element.text)
    except ET.ParseError:
        pass
    return urls


def cleanup_temp_directories(directories_to_clean: Optional[List[str]] = None):
    """
    Cleans up temporary directories created during runs.
    By default, cleans 'temp/browser_sessions' and 'temp/browser_sessions_pool'.
    """
    if directories_to_clean is None:
        directories_to_clean = [TEMP_BROWSER_SESSIONS_DIR, TEMP_BROWSER_SESSIONS_POOL_DIR]

    print("--- Starting Temporary Directory Cleanup ---")
    
    # First, try to kill any lingering Chrome processes
    try:
        import subprocess
        import psutil
        
        # Try using psutil first for more reliable process killing
        killed_any = False
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if proc.info['name'] and ('chrome' in proc.info['name'].lower() or 'chromedriver' in proc.info['name'].lower()):
                    proc.kill()
                    killed_any = True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        if killed_any:
            time.sleep(2)  # Give processes time to terminate
    except ImportError:
        # Fallback if psutil not available
        try:
            # Kill Chrome and chromedriver processes on Windows
            if os.name == 'nt':
                subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe'], capture_output=True)
                subprocess.run(['taskkill', '/F', '/IM', 'chromedriver.exe'], capture_output=True)
                time.sleep(1)  # Give processes time to terminate
        except:
            pass  # Ignore errors if processes don't exist
    
    # Clean up the base directory by looking for old session directories
    base_dir = os.path.dirname(TEMP_BROWSER_SESSIONS_POOL_DIR)
    if os.path.exists(base_dir):
        for item in os.listdir(base_dir):
            if item.startswith('browser_sessions_pool_'):
                old_dir = os.path.join(base_dir, item)
                try:
                    shutil.rmtree(old_dir)
                    print(f"Cleaned up old session directory: {old_dir}")
                except Exception as e:
                    print(f"Could not remove {old_dir}: {e}")
    
    # Also clean up the directories passed in
    for directory in directories_to_clean:
        dir_path = directory  # Now using absolute paths from config
        if os.path.exists(dir_path):
            # Try multiple times with delays for Windows file locking
            for attempt in range(3):
                try:
                    print(f"Removing directory: {dir_path} (attempt {attempt + 1}/3)")
                    shutil.rmtree(dir_path)
                    print(f"Successfully removed {dir_path}")
                    break
                except Exception as e:
                    if attempt < 2:
                        print(f"Error removing directory {dir_path}: {e}")
                        print("Waiting 2 seconds before retry...")
                        time.sleep(2)
                    else:
                        # Final attempt - just log the error
                        print(f"Failed to remove directory {dir_path}: {e}")
                        print("Directory will remain for manual cleanup")
        else:
            print(f"Directory not found, skipping: {dir_path}")
    print("--- Cleanup Complete ---")


def send_discord_embed(webhook_url: str, embed_data: Dict[str, Any], username: str = "Walmart Scraper Bot") -> Optional[Dict[str, Any]]:
    """
    Sends a Discord embed message via webhook.
    
    Parameters
    ----------
    webhook_url : str
        The Discord webhook URL
    embed_data : Dict[str, Any]
        The embed data containing color, title, description, footer, etc.
    username : str
        The username to display for the bot
        
    Returns
    -------
    Optional[Dict[str, Any]]
        Response data including message ID if successful, None if failed
    """
    if not webhook_url:
        return None
        
    # Ensure timestamp is in ISO 8601 format
    if "timestamp" not in embed_data:
        embed_data["timestamp"] = datetime.utcnow().isoformat()
    
    payload = {
        "username": username,
        "embeds": [embed_data]
    }
    
    # Add ?wait=true to get message ID in response
    if '?' in webhook_url:
        full_url = webhook_url + '&wait=true'
    else:
        full_url = webhook_url + '?wait=true'
        
    try:
        response = requests.post(full_url, json=payload, timeout=10)
        if response.status_code in [200, 204]:
            # Get message ID from response if available
            if response.text:
                try:
                    return response.json()
                except:
                    # Response text exists but isn't valid JSON
                    return {"success": True, "text": response.text}
            return {"success": True}
        elif response.status_code == 429:
            print("Discord rate limit hit, skipping this message")
            return None
        else:
            print(f"Discord webhook error: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.Timeout:
        print("Discord webhook timeout, skipping this message")
        return None
    except Exception as e:
        print(f"Error sending Discord embed: {e}")
        return None


def edit_discord_embed(webhook_url: str, message_id: str, embed_data: Dict[str, Any], username: str = "Walmart Scraper Bot") -> bool:
    """
    Edits an existing Discord embed message via webhook.
    
    Parameters
    ----------
    webhook_url : str
        The Discord webhook URL
    message_id : str
        The ID of the message to edit
    embed_data : Dict[str, Any]
        The new embed data
    username : str
        The username to display for the bot
        
    Returns
    -------
    bool
        True if successful, False otherwise
    """
    if not webhook_url or not message_id:
        return False
        
    # Extract webhook ID and token from URL
    import re
    match = re.match(r'https://discord\.com/api/webhooks/(\d+)/(.+)', webhook_url)
    if not match:
        print("Invalid webhook URL format")
        return False
        
    webhook_id, webhook_token = match.groups()
    
    # Ensure timestamp is updated
    if "timestamp" not in embed_data:
        embed_data["timestamp"] = datetime.utcnow().isoformat()
    
    # Discord API endpoint for editing webhook messages
    edit_url = f"https://discord.com/api/webhooks/{webhook_id}/{webhook_token}/messages/{message_id}"
    
    payload = {
        "embeds": [embed_data]
    }
    
    try:
        response = requests.patch(edit_url, json=payload, timeout=10)
        if response.status_code == 200:
            return True
        elif response.status_code == 429:
            print("Discord rate limit hit, skipping edit")
            return False
        else:
            print(f"Discord edit error: {response.status_code} - {response.text}")
            return False
    except requests.exceptions.Timeout:
        print("Discord edit timeout, skipping edit")
        return False
    except Exception as e:
        print(f"Error editing Discord embed: {e}")
        return False


def create_embed_data(
    title: str,
    description: str = "",
    color: int = 0x3498db,  # Default blue color
    footer_text: str = "",
    fields: Optional[List[Dict[str, Any]]] = None,
    thumbnail_url: Optional[str] = None,
    include_timestamp: bool = True
) -> Dict[str, Any]:
    """
    Creates a properly formatted embed data structure for Discord.
    
    Parameters
    ----------
    title : str
        The embed title
    description : str
        The embed description
    color : int
        The color as a decimal integer (e.g., 0x3498db for blue)
    footer_text : str
        Text to display in the footer
    fields : Optional[List[Dict[str, Any]]]
        List of field dictionaries with 'name', 'value', and optional 'inline' keys
    thumbnail_url : Optional[str]
        URL for thumbnail image
    include_timestamp : bool
        Whether to include current timestamp
        
    Returns
    -------
    Dict[str, Any]
        Formatted embed data
    """
    embed = {
        "title": title,
        "description": description,
        "color": color
    }
    
    if footer_text:
        embed["footer"] = {"text": footer_text}
    
    if fields:
        embed["fields"] = fields
        
    if thumbnail_url:
        embed["thumbnail"] = {"url": thumbnail_url}
        
    if include_timestamp:
        embed["timestamp"] = datetime.utcnow().isoformat()
    
    return embed


class DiscordProgressTracker:
    """Helper class to track and update crawler progress in Discord."""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = self._validate_webhook_url(webhook_url)
        self.initial_message_id = None  # ID of the initial setup message
        self.progress_message_id = None  # ID of the progress tracking message
        self.total_items = 0
        self.completed_items = 0
        self.failed_items = 0
        self.current_url = ""
        self.start_time = None
        self.last_update_time = None
        
    def _validate_webhook_url(self, webhook_url: str) -> str:
        """Validate and clean the webhook URL."""
        if not webhook_url:
            raise ValueError("Discord webhook URL is empty or None")
            
        # Check if it contains placeholder values
        if "None" in webhook_url or webhook_url.endswith("/None"):
            raise ValueError("Discord webhook URL contains 'None' - check your .env configuration")
            
        # Validate URL format
        if not webhook_url.startswith("https://discord.com/api/webhooks/"):
            raise ValueError("Invalid Discord webhook URL format")
            
        return webhook_url
        
    def send_initial_embed(self, spider_name: str, settings=None, proxy_count=None):
        """Send the initial crawler started embed with technical information."""
        self.start_time = datetime.utcnow()
        
        # Extract technical settings
        browser_pool_size = settings.get('BROWSER_POOL_SIZE', 'N/A') if settings else 'N/A'
        concurrent_requests = settings.get('CONCURRENT_REQUESTS', 'N/A') if settings else 'N/A'
        concurrent_per_domain = settings.get('CONCURRENT_REQUESTS_PER_DOMAIN', 'N/A') if settings else 'N/A'
        download_delay = settings.get('DOWNLOAD_DELAY', 'N/A') if settings else 'N/A'
        retry_times = settings.get('RETRY_TIMES', 'N/A') if settings else 'N/A'
        autothrottle = "Enabled" if settings and settings.get('AUTOTHROTTLE_ENABLED') else "Disabled"
        autothrottle_target = settings.get('AUTOTHROTTLE_TARGET_CONCURRENCY', 'N/A') if settings else 'N/A'
        autothrottle_max_delay = settings.get('AUTOTHROTTLE_MAX_DELAY', 'N/A') if settings else 'N/A'
        
        embed = create_embed_data(
            title=f":rocket: Crawler Initiated: {spider_name}",
            description=(
                ":spider_web: **Walmart Scraper Bot is starting up!**\n\n"
                ":gear: Initializing browser pool...\n"
                ":globe_with_meridians: Loading proxy configuration...\n"
                ":hourglass: Preparing to crawl..."
            ),
            color=0x2ecc71,  # Green
            footer_text="Walmart Scraper Bot",
            fields=[
                {
                    "name": ":desktop: Browser Configuration",
                    "value": f"**Pool Size:** {browser_pool_size} browsers\n**Mode:** Headless Chrome\n**User-Agent Rotation:** Enabled",
                    "inline": True
                },
                {
                    "name": ":zap: Concurrency Settings",
                    "value": f"**Total:** {concurrent_requests} requests\n**Per Domain:** {concurrent_per_domain} requests\n**Target:** {autothrottle_target}",
                    "inline": True
                },
                {
                    "name": ":timer: Timing Configuration",
                    "value": f"**Base Delay:** {download_delay}s\n**AutoThrottle:** {autothrottle}\n**Max Delay:** {autothrottle_max_delay}s",
                    "inline": True
                },
                {
                    "name": ":globe_with_meridians: Proxy Configuration",
                    "value": f"**Available Proxies:** {proxy_count if proxy_count else 'Unknown'}\n**Rotation:** Round-robin\n**Failure Tracking:** Enabled",
                    "inline": True
                },
                {
                    "name": ":repeat: Retry Configuration",
                    "value": f"**Max Retries:** {retry_times} attempts\n**Bot Detection:** Auto-retry\n**Proxy Swap:** On failure",
                    "inline": True
                },
                {
                    "name": ":shield: Anti-Detection Features",
                    "value": (
                        "• Random User-Agent rotation\n"
                        "• Browser fingerprint evasion\n"
                        "• Human-like mouse movements\n"
                        "• Random scroll patterns\n"
                        "• Page load waiting\n"
                        "• Cookie management"
                    ),
                    "inline": False
                }
            ]
        )
        
        response = send_discord_embed(self.webhook_url, embed)
        if response:
            # Store the initial message ID
            self.initial_message_id = response.get("id") or response.get("message", {}).get("id")
            if not self.initial_message_id and response.get("text"):
                try:
                    import json
                    if response["text"].startswith("{"):
                        parsed = json.loads(response["text"])
                        self.initial_message_id = parsed.get("id")
                except:
                    pass
            
            if self.initial_message_id:
                print(f"Discord initial message sent successfully (ID: {self.initial_message_id})")
            else:
                print("Warning: Could not get initial message ID from Discord response")
                
        # Now send a separate progress tracking message
        self._send_initial_progress_message(spider_name)
            
    def _send_initial_progress_message(self, spider_name: str):
        """Send a separate message for progress tracking."""
        embed = create_embed_data(
            title=f":hourglass_flowing_sand: Crawling in Progress: {spider_name}",
            description=(
                ":gear: **Initializing crawler...**\n\n"
                ":mag: Preparing to scan Walmart store locations\n"
                ":globe_with_meridians: Loading proxy configuration\n"
                ":timer_clock: Waiting for first requests..."
            ),
            color=0xffa500,  # Orange for "in progress"
            footer_text="Live Progress Tracking"
        )
        
        response = send_discord_embed(self.webhook_url, embed)
        if response:
            self.progress_message_id = response.get("id") or response.get("message", {}).get("id")
            if not self.progress_message_id and response.get("text"):
                try:
                    import json
                    if response["text"].startswith("{"):
                        parsed = json.loads(response["text"])
                        self.progress_message_id = parsed.get("id")
                except:
                    pass
            
            if self.progress_message_id:
                print(f"Discord progress tracking message sent (ID: {self.progress_message_id})")
            else:
                print("Warning: Could not get progress message ID - live updates disabled")
        
    def update_progress(self, current_url: str, total: int, completed: int, failed: int):
        """Update the progress embed with current crawling status."""
        if not self.progress_message_id:
            return
            
        # Rate limiting - don't update more than once every 5 seconds
        current_time = datetime.utcnow()
        if self.last_update_time and (current_time - self.last_update_time).total_seconds() < 5:
            return
            
        self.current_url = current_url
        self.total_items = total
        self.completed_items = completed
        self.failed_items = failed
        self.last_update_time = current_time
        
        # Calculate percentage
        percentage = (completed / total * 100) if total > 0 else 0
        
        # Create progress bar
        progress_bar = self._create_progress_bar(percentage)
        
        # Format current URL for display
        display_url = current_url.replace("https://www.walmart.com", "")
        
        embed = create_embed_data(
            title=":shopping_cart: Crawling Walmart Stores",
            description=(
                f":link: **Current URL:** `{display_url}`\n\n"
                f"{progress_bar}\n\n"
                f":white_check_mark: **Completed:** {completed}/{total} ({percentage:.1f}%)\n"
                f":x: **Failed:** {failed} attempts\n"
                f":hourglass: **In Progress:** {total - completed}"
            ),
            color=0x3498db if failed < 10 else 0xe74c3c,  # Blue if ok, red if many failures
            footer_text=f"Started at {self.start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}" if self.start_time else "Walmart Scraper Bot"
        )
        
        # Add fields for additional stats
        if failed > 0:
            embed["fields"] = [
                {
                    "name": ":warning: Recent Failures",
                    "value": f"{failed} requests failed with bot detection",
                    "inline": True
                },
                {
                    "name": ":repeat: Retry Status",
                    "value": "Retrying with new proxies...",
                    "inline": True
                }
            ]
        
        edit_discord_embed(self.webhook_url, self.progress_message_id, embed)
        
    def _create_progress_bar(self, percentage: float) -> str:
        """Create a visual progress bar using Discord formatting."""
        filled = int(percentage / 5)  # 20 segments total
        empty = 20 - filled
        
        bar = "█" * filled + "░" * empty
        return f"**Progress:** [{bar}] {percentage:.1f}%"
        
    def send_completion_embed(self, spider_name: str, items_scraped: int):
        """Send a completion embed when spider finishes."""
        duration = ""
        if self.start_time:
            elapsed = datetime.utcnow() - self.start_time
            duration = f"{elapsed.total_seconds():.1f} seconds"
        
        # Update the progress message to show completion
        if self.progress_message_id:
            embed = create_embed_data(
                title=f":white_check_mark: Crawling Complete: {spider_name}",
                description=(
                    f":trophy: **Scraping completed successfully!**\n\n"
                    f":package: **Total Items Scraped:** {items_scraped}\n"
                    f":white_check_mark: **Successful Requests:** {self.completed_items}\n"
                    f":x: **Failed Requests:** {self.failed_items}\n"
                    f":stopwatch: **Duration:** {duration}"
                ),
                color=0x2ecc71 if self.failed_items < self.completed_items else 0xe74c3c,
                footer_text="Crawling Completed"
            )
            
            edit_discord_embed(self.webhook_url, self.progress_message_id, embed)
        
        # Also send a separate completion notification
        final_embed = create_embed_data(
            title=f":tada: Crawler Finished: {spider_name}",
            description=(
                f"**Crawling session completed!**\n\n"
                f":chart_with_upwards_trend: **Final Statistics:**\n"
                f"• Items scraped: **{items_scraped}**\n"
                f"• Duration: **{duration}**\n"
                f"• Success rate: **{(self.completed_items/(self.completed_items + self.failed_items)*100):.1f}%**" if (self.completed_items + self.failed_items) > 0 else ""
            ),
            color=0x2ecc71,
            footer_text="Walmart Scraper Bot"
        )
        
        send_discord_embed(self.webhook_url, final_embed)


def check_discord_config() -> bool:
    """
    Check if Discord configuration is properly set up.
    
    Returns
    -------
    bool
        True if Discord is configured, False otherwise
    """
    try:
        # Try to load discord config
        from helpers.discord_config import get_webhook_url
        webhook_url = get_webhook_url()
        
        if not webhook_url:
            return False
            
        # Basic validation
        if "None" in webhook_url or not webhook_url.startswith("https://discord.com/api/webhooks/"):
            return False
            
        return True
    except ImportError:
        print("Discord config not found - Discord notifications disabled")
        return False
    except Exception as e:
        print(f"Discord config error: {e}")
        return False