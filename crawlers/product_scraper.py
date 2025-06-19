import json
import os
import time
import random
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from helpers.helpers import ensure_dir, make_request_with_retries, extract_next_data, ProxyManager


class ProductScraper:
    """
    Scrapes product data from Walmart category pages using the __NEXT_DATA__ JSON blob.
    """

    def __init__(
        self,
        pathway_file: str = "data/pathway.json",
        output_dir: str = "data",
        verbose: bool = True,
    ):
        self.pathway_file = pathway_file
        self.output_dir = output_dir
        self.verbose = verbose
        self.proxy_manager = ProxyManager()
        ensure_dir(self.output_dir)

    def _load_pathway(self) -> List[str]:
        """Load the category URLs from the pathway file."""
        try:
            with open(self.pathway_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: Pathway file not found at {self.pathway_file}")
            return []
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from {self.pathway_file}")
            return []

    def _find_in_dict(self, data: Dict[str, Any], key_to_find: str) -> Optional[Any]:
        """Recursively search a dictionary for a specific key."""
        if key_to_find in data:
            return data[key_to_find]
        for key, value in data.items():
            if isinstance(value, dict):
                item = self._find_in_dict(value, key_to_find)
                if item is not None:
                    return item
            elif isinstance(value, list):
                for i in value:
                    if isinstance(i, dict):
                        item = self._find_in_dict(i, key_to_find)
                        if item is not None:
                            return item
        return None

    def _extract_products_from_next_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract product items from the __NEXT_DATA__ structure."""
        # The path to product data can be brittle. We search for it recursively.
        # Based on analysis, item stacks are a good indicator.
        def find_item_stacks(d):
            if isinstance(d, dict):
                if "itemStacks" in d and isinstance(d["itemStacks"], list):
                    return d["itemStacks"]
                for value in d.values():
                    found = find_item_stacks(value)
                    if found:
                        return found
            elif isinstance(d, list):
                for item in d:
                    found = find_item_stacks(item)
                    if found:
                        return found
            return None

        item_stacks = find_item_stacks(data)
        if not item_stacks:
            return []

        products = []
        for stack in item_stacks:
            products.extend(stack.get("items", []))
        return products

    def run(self):
        """Main execution method to scrape all categories."""
        category_urls = self._load_pathway()
        if not category_urls:
            print("No category URLs found. Aborting.")
            return

        print(f"Found {len(category_urls)} categories to scrape.")
        if self.proxy_manager.is_available():
            print(f"Using proxy pool of {len(self.proxy_manager.proxies)} proxies.")

        for url in category_urls:
            self.scrape_category(url)
            # Add a small delay between categories to be polite
            time.sleep(random.uniform(1, 3))

    def scrape_category(self, category_url: str):
        """Scrapes all pages for a single category."""
        if self.verbose:
            print(f"\nScraping category: {category_url}")

        # --- Scrape first page ---
        response = make_request_with_retries("GET", category_url, timeout=15, proxies=self.proxy_manager.get_random_proxy_dict())
        if not response:
            print(f"Failed to fetch category page: {category_url}")
            return

        next_data = extract_next_data(response.text)
        if not next_data:
            return

        all_products = self._extract_products_from_next_data(next_data)
        if self.verbose:
            print(f"  > Found {len(all_products)} products on page 1.")

        # --- Handle pagination ---
        pagination_info = self._find_in_dict(next_data, "paginationV2")
        max_page = pagination_info.get("maxPage", 1) if pagination_info else 1

        if self.verbose:
            print(f"  > Total pages for this category: {max_page}")

        if max_page > 1:
            for page_num in range(2, max_page + 1):
                page_url = f"{category_url}?page={page_num}"
                if self.verbose:
                    print(f"  > Scraping page {page_num}: {page_url}")
                
                # Add a small delay between pages
                time.sleep(random.uniform(0.5, 1.5))
                
                response = make_request_with_retries("GET", page_url, timeout=15, proxies=self.proxy_manager.get_random_proxy_dict())
                if not response:
                    print(f"    ! Failed to fetch page {page_num}. Skipping.")
                    continue
                
                next_data = extract_next_data(response.text)
                if not next_data:
                    print(f"    ! Could not find __NEXT_DATA__ on page {page_num}. Skipping.")
                    continue
                
                page_products = self._extract_products_from_next_data(next_data)
                if self.verbose:
                    print(f"    > Found {len(page_products)} products on page {page_num}.")
                all_products.extend(page_products)

        # Save results to a file named after the category
        category_slug = category_url.strip("/").split("/")[-1]
        output_file = os.path.join(self.output_dir, f"{category_slug}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_products, f, indent=2)

        if self.verbose:
            print(f"  > Finished. Saved {len(all_products)} total products to {output_file}")


if __name__ == "__main__":
    scraper = ProductScraper()
    scraper.run() 