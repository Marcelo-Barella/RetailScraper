import json
import os
import time
from typing import List, Dict, Any, Set, Optional

from helpers.helpers import ensure_dir, fetch_content_browser, extract_next_data, ProxyManager, cleanup_temp_directories


class CategoryPathBuilder:
    """
    Builds the 'pathway.json' file by fetching the Walmart homepage,
    parsing the __NEXT_DATA__ JSON blob, and extracting category links.
    """

    HOMEPAGE_URL = "https://www.walmart.com"
    BASE_URL = "https://www.walmart.com"

    def __init__(self, data_path: str = "data", verbose: bool = True):
        self.output_path = os.path.join(data_path, "pathway.json")
        self.verbose = verbose
        self.proxy_manager = ProxyManager()
        ensure_dir(data_path)

    def _fetch_homepage_html(self) -> Optional[str]:
        """Fetch the homepage HTML using a headless browser."""
        if self.verbose:
            print(f"Fetching homepage from {self.HOMEPAGE_URL}...")
        
        proxy = self.proxy_manager.get_random_proxy()
        if proxy and self.verbose:
            print(f"Using proxy for browser session: {proxy}")

        return fetch_content_browser(self.HOMEPAGE_URL, wait_seconds=10, proxy=proxy)

    def _walk_next_data_for_categories(self, data: Any, discovered_paths: Set[str]):
        """Recursively search the __NEXT_DATA__ blob for category links."""
        if isinstance(data, dict):
            # As per rules, a category object has 'name' and 'path'/'url'
            name = data.get("name")
            path = data.get("path") or data.get("url")

            if name and isinstance(name, str) and path and isinstance(path, str):
                # Heuristic to identify category-like paths
                if path.startswith("/browse/") or path.startswith("/cp/"):
                    full_url = f"{self.BASE_URL}{path.split('?')[0]}"
                    discovered_paths.add(full_url)
            
            for value in data.values():
                self._walk_next_data_for_categories(value, discovered_paths)

        elif isinstance(data, list):
            for item in data:
                self._walk_next_data_for_categories(item, discovered_paths)

    def run(self) -> None:
        """Main execution method."""
        start_time = time.time()
        
        html_content = self._fetch_homepage_html()
        if not html_content:
            print("Could not fetch homepage HTML. Aborting.")
            return

        next_data = extract_next_data(html_content)
        if not next_data:
            print("Could not extract __NEXT_DATA__ from homepage. Aborting.")
            debug_path = os.path.join("debug", "homepage_raw.html")
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            print(f"Saved raw HTML to {debug_path} for analysis.")
            return

        discovered_paths: Set[str] = set()
        self._walk_next_data_for_categories(next_data, discovered_paths)

        if not discovered_paths:
            print("Did not discover any category paths from __NEXT_DATA__.")
            debug_path = os.path.join("debug", "homepage_next_data.json")
            with open(debug_path, "w", encoding="utf-8") as f:
                json.dump(next_data, f, indent=2)
            print(f"Dumping the entire JSON to {debug_path} for analysis.")
            return

        final_paths = sorted(list(discovered_paths))
        if self.verbose:
            print(f"Discovered {len(final_paths)} unique category paths.")
        
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(final_paths, f, indent=2)

        end_time = time.time()
        print(f"Successfully generated {self.output_path} in {end_time - start_time:.2f} seconds.")


def main():
    """Main execution function to build the category paths."""
    print("Starting category path builder...")
    # Clean up any leftover directories from previous crashed runs
    cleanup_temp_directories()
    
    builder = CategoryPathBuilder()
    builder.run()


if __name__ == "__main__":
    main() 