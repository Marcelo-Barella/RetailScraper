import json
import os
import subprocess
import tempfile
import sys
import time
from typing import List, Dict, Set, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add the project root to the Python path to allow imports from other directories
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Import our hardened browser fetcher for realistic validation
from helpers.helpers import fetch_content_browser, ensure_dir, cleanup_temp_directories
from helpers.config import TEMP_BASE_DIR
from crawlers.free_proxy_spider import FreeProxySpider
from scrapy.crawler import CrawlerProcess

# Configuration
REMOTE_PROXY_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.json"
OUTPUT_FILE = os.path.join("helpers", "proxies.json")
VALIDATION_TARGET_URL = "https://www.walmart.com/store-directory" # A realistic, tough target
# Concurrency for browser validation must be much lower to avoid resource exhaustion.
# Adjust this based on your system's memory and CPU capacity.
BROWSER_VALIDATION_CONCURRENCY = 10
BROWSER_VALIDATION_TIMEOUT_PER_PROXY = 45 # Increased timeout for browser


def run_free_proxy_spider() -> List[Dict[str, str]]:
    """
    Run the FreeProxySpider to scrape proxies from various websites.
    """
    print("🕷️  Running FreeProxySpider to gather initial proxy list...")

    # Ensure a temporary directory exists within the project to avoid system-level errors
    temp_dir = TEMP_BASE_DIR
    ensure_dir(temp_dir)

    # Use a temporary file to store the spider's output
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False, dir=temp_dir) as temp_file:
        temp_filename = temp_file.name

    settings = {
        "FEEDS": {
            temp_filename: {"format": "json", "overwrite": True, "encoding": "utf8"}
        },
        "LOG_LEVEL": "INFO",
    }

    process = CrawlerProcess(settings)
    print("--- Running FreeProxySpider to scrape sources ---")
    process.crawl(FreeProxySpider)
    process.start()

    print(f"--- Spider finished. Reading proxies from {temp_filename} ---")
    try:
        with open(temp_filename, 'r', encoding='utf-8') as f:
            proxies = json.load(f)
        return proxies
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Could not read or decode spider output from temp file: {e}")
        return []
    finally:
        try:
            os.unlink(temp_filename)
        except OSError as e:
            print(f"Error removing temp file {temp_filename}: {e}")


def download_remote_proxies() -> List[Dict[str, str]]:
    """
    Download a list of proxies from the specified remote URL using curl.
    """
    print(f"--- Downloading additional proxies from: {REMOTE_PROXY_URL} ---")
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as temp_file:
        temp_path = temp_file.name
    
    try:
        result = subprocess.run(
            ['curl', '-sL', '--connect-timeout', '15', REMOTE_PROXY_URL, '-o', temp_path],
            capture_output=True, text=True, timeout=60, check=False
        )
        if result.returncode != 0:
            print(f"Error downloading proxies with curl: {result.stderr}")
            return []
        
        with open(temp_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except subprocess.TimeoutExpired:
        print("Error: Download via curl timed out after 60 seconds.")
        return []
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error reading or parsing downloaded proxy file: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred during proxy download: {e}")
        return []
    finally:
        try:
            os.unlink(temp_path)
        except OSError as e:
            print(f"Error removing temp file {temp_path}: {e}")


def convert_to_url_set(proxy_list: List[Dict]) -> Set[str]:
    """
    Converts a list of proxy dicts into a set of unique proxy URLs.
    """
    urls = set()
    for p in proxy_list:
        if 'proxy' in p and p['proxy']:
            urls.add(p['proxy'])
        elif 'ip' in p and 'port' in p:
            protocol = "https" if str(p.get('https', 'no')).lower() in {'yes', 'true', '1'} else "http"
            urls.add(f"{protocol}://{p['ip']}:{p['port']}")
    return urls


def test_proxy_with_browser(proxy_url: str, verbose: bool = False) -> Optional[str]:
    """
    Return the proxy_url if it can connect to the target URL using our hardened browser,
    otherwise None. This is slow but highly accurate.
    """
    if verbose:
        print(f"  [TESTING] {proxy_url} with full browser...")

    html_content = fetch_content_browser(
        VALIDATION_TARGET_URL,
        proxy=proxy_url,
        wait_seconds=20,  # Give enough time for CAPTCHA pages to appear
        headless=True
    )

    if html_content:
        # fetch_content_browser returns None on failure (including CAPTCHAs)
        print(f"  [SUCCESS] {proxy_url}")
        return proxy_url
    else:
        if verbose:
            print(f"  [FAIL] {proxy_url}")
        return None


def validate_proxy_list(proxy_urls: Set[str], verbose: bool = False) -> List[str]:
    """
    Test all proxies in the list using parallel browser instances and return the working ones.
    """
    working_proxies = []
    
    print(f"\n--- Testing {len(proxy_urls)} proxies using up to {BROWSER_VALIDATION_CONCURRENCY} parallel browsers ---")
    print("(This process is slow but ensures proxies work against bot detection.)")

    with ThreadPoolExecutor(max_workers=BROWSER_VALIDATION_CONCURRENCY) as executor:
        futures = {executor.submit(test_proxy_with_browser, url, verbose): url for url in proxy_urls}
        
        from tqdm import tqdm
        for future in tqdm(as_completed(futures), total=len(proxy_urls), desc="Validating Proxies"):
            result = future.result()
            if result:
                working_proxies.append(result)

    return working_proxies


def main(verbose_validation: bool = False):
    """Main execution function to get and validate proxies."""
    # Clean up any leftover directories from previous crashed runs
    cleanup_temp_directories()

    # 1. Gather proxies from all sources
    scraped_proxies = run_free_proxy_spider()
    remote_proxies = download_remote_proxies()

    # 2. Combine and deduplicate
    all_proxy_urls = convert_to_url_set(scraped_proxies)
    all_proxy_urls.update(convert_to_url_set(remote_proxies))

    if not all_proxy_urls:
        print("No proxy candidates found from any source. Aborting.")
        return

    print(f"\n--- Found {len(all_proxy_urls)} unique proxy candidates. Starting browser-based validation... ---")
    
    # 3. Validate the combined list using the browser-based method
    working_proxies = validate_proxy_list(all_proxy_urls, verbose=verbose_validation)

    print(f"\n--- Validation complete. {len(working_proxies)} proxies are working. ---")

    # 4. Save the results
    if not working_proxies:
        print(f"No working proxies found. '{OUTPUT_FILE}' will not be updated.")
        return

    output_data = {"proxies": [{"proxy": p} for p in working_proxies]}
    
    ensure_dir(os.path.dirname(OUTPUT_FILE))
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)

    print(f"Successfully saved {len(working_proxies)} working proxies to '{OUTPUT_FILE}'.")


if __name__ == "__main__":
    import argparse

    # Clean up any leftover directories from previous crashed runs
    cleanup_temp_directories()

    parser = argparse.ArgumentParser(description="Get and validate free proxies.")
    parser.add_argument("--max-proxies", type=int, default=50, help="Maximum number of proxies to test.")
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help="Enable verbose output to see the status of each proxy during validation."
    )
    args = parser.parse_args()

    main(verbose_validation=args.verbose) 