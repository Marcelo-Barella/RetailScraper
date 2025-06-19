import random
from scrapy import signals
from scrapy.http.headers import Headers

# Common header keys used by real browsers (HTTP/2 pseudo-headers excluded)
_HEADER_KEYS = [
    "accept",
    "accept-language",
    "sec-ch-ua",
    "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
    "user-agent",
    "sec-fetch-site",
    "sec-fetch-mode",
    "sec-fetch-dest",
    "sec-fetch-user",
    "upgrade-insecure-requests",
]


class HeaderOrderRandomizerMiddleware:
    """Randomises request header order & optionally removes low-value headers.

    Walmart (via Akamai) fingerprints exact header sequencing. Rotating the
    order across a small, realistic permutation space makes fingerprints far
    less stable while preserving semantic meaning.
    """

    def __init__(self, remove_probability: float = 0.1):
        self.remove_probability = remove_probability

    @classmethod
    def from_crawler(cls, crawler):
        # Let the probability be configurable via settings
        prob = crawler.settings.getfloat("HEADER_RANDOMIZER_REMOVE_PROB", 0.1)
        mw = cls(remove_probability=prob)
        return mw

    def process_request(self, request, spider):
        # Convert to dict of unicode -> list for manipulation
        header_items = list(request.headers.items())
        if not header_items:
            return

        # Build an ordered list we care about first; keep any extra custom headers at the end
        main_headers = [h for h in header_items if h[0].decode().lower() in _HEADER_KEYS]
        extra_headers = [h for h in header_items if h[0].decode().lower() not in _HEADER_KEYS]

        # Potentially remove some low-value headers to vary signature
        pruned = []
        for k, v in main_headers:
            if random.random() < self.remove_probability and k.decode().lower() in {"sec-fetch-user", "upgrade-insecure-requests"}:
                continue  # drop it
            pruned.append((k, v))

        # Shuffle the remaining headers (Fisher-Yates)
        random.shuffle(pruned)

        # Combine back with extras (extras keep existing order)
        final_headers = pruned + extra_headers

        # Re-assign while preserving new ordering
        new_headers = Headers()
        for k, v in final_headers:
            new_headers[k] = v
        request.headers = new_headers 