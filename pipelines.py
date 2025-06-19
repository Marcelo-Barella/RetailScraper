import os
import json
import scrapy
from scrapy.exceptions import DropItem


class StoreDedupPipeline:
    """Drop items whose href already exists in the output file.

    Works with jsonlines output (one JSON object per line).
    """

    def __init__(self, output_path: str = "data/stores.jl"):
        self.output_path = output_path
        self.seen = set()

    @classmethod
    def from_crawler(cls, crawler):
        # Path can be overridden through settings if needed
        path = crawler.settings.get("STORES_OUTPUT_PATH", "data/stores.jl")
        return cls(output_path=path)

    def open_spider(self, spider):
        # Pre-load already stored hrefs so we can skip duplicates
        if os.path.exists(self.output_path):
            with open(self.output_path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, dict):
                            href = obj.get("href")
                            if href:
                                self.seen.add(href)
                    except json.JSONDecodeError:
                        continue

    def process_item(self, item, spider):
        href = item.get("href")
        if href in self.seen:
            raise DropItem(f"Duplicate store skipped: {href}")
        self.seen.add(href)
        return item


class StoreStreamJSONPipeline:
    """Incrementally writes each unique store item into a JSON array (stores.json) as the spider runs."""

    def __init__(self, output_path: str = "data/stores.json"):
        self.output_path = output_path
        self.file = None
        self.first_record = True

    @classmethod
    def from_crawler(cls, crawler):
        path = crawler.settings.get("STORES_OUTPUT_PATH", "data/stores.json")
        return cls(output_path=path)

    def open_spider(self, spider):
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        # Overwrite file for a fresh run
        self.file = open(self.output_path, "w", encoding="utf-8")
        self.file.write("[\n")
        self.first_record = True

    def process_item(self, item, spider):
        # Write comma separator if this is not the first element
        if not self.first_record:
            self.file.write(",\n")
        json.dump(dict(item), self.file, ensure_ascii=False)
        self.file.flush()
        self.first_record = False
        return item

    def close_spider(self, spider):
        # Close the JSON array
        self.file.write("\n]\n")
        self.file.close() 