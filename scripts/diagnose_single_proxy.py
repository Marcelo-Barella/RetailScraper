import argparse
import sys
import os
import json

# Add project root to path to allow helper imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from helpers.helpers import fetch_content_browser, ensure_dir

def get_first_proxy_from_file(proxy_file: str = "helpers/proxies.json") -> str | None:
    """A simple function to read the first proxy from the list."""
    try:
        with open(proxy_file, "r") as f:
            data = json.load(f).get("proxies", [])
            if data and "proxy" in data[0]:
                return data[0]["proxy"]
    except (FileNotFoundError, IndexError, json.JSONDecodeError):
        return None
    return None

def main():
    parser = argparse.ArgumentParser(
        description="Test a single proxy against Walmart using undetectable-chromedriver."
    )
    parser.add_argument(
        "--proxy",
        type=str,
        default=None,
        help="The full proxy URL to test (e.g., 'http://1.2.3.4:8080'). If not provided, the script will try to use the first proxy from helpers/proxies.json.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run the browser in headed mode for visual inspection.",
    )
    args = parser.parse_args()

    proxy_to_test = args.proxy
    if not proxy_to_test:
        print("No proxy provided via argument, attempting to read from 'helpers/proxies.json'...")
        proxy_to_test = get_first_proxy_from_file()

    if not proxy_to_test:
        print("\n❌ ERROR: No proxy could be found.")
        print("Please provide a proxy via the --proxy argument or ensure 'helpers/proxies.json' contains a valid proxy list.")
        sys.exit(1)

    print("\n--- Starting Single Proxy Diagnosis ---")
    print(f"Proxy: {proxy_to_test}")
    print(f"Target: https://www.walmart.com")
    print(f"Mode: {'Headed' if args.headed else 'Headless'}")
    print("-" * 40)

    # Use the existing browser-fetching function from helpers
    html_content = fetch_content_browser(
        url="https://www.walmart.com",
        proxy=proxy_to_test,
        headless=not args.headed,
        wait_seconds=30 # Give it ample time
    )

    print("-" * 40)
    if html_content:
        print("✅ SUCCESS: The proxy appears to be working.")
        print("The browser was able to fetch the page content successfully.")
        # Save the content for inspection
        ensure_dir("debug")
        with open("debug/proxy_diag_success.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        print("Saved successful page content to 'debug/proxy_diag_success.html'")
    else:
        print("❌ FAILURE: The proxy failed to connect or was blocked.")
        print("The browser could not retrieve the page content. This strongly suggests the proxy is not viable for this target.")
    print("--- Diagnosis Complete ---\n")

if __name__ == "__main__":
    main() 