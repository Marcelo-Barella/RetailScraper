# Scrapy settings for the retailScraper project

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Default headers to mimic a real browser
DEFAULT_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Dnt": "1",
}

# Configure the JSON output feed
# FEEDS = {
#     'data/stores.jl': {
#         'format': 'jsonlines',
#         'encoding': 'utf8',
#         'store_empty': False,
#         'overwrite': False,
#     }
# }

# Path used by pipelines for incremental store JSON output
STORES_OUTPUT_PATH = 'data/stores.json'

# Item pipelines: dedup first, then stream to JSON
ITEM_PIPELINES = {
    'pipelines.StoreDedupPipeline': 100,
    'pipelines.StoreStreamJSONPipeline': 200,
}

# --- PERFORMANCE OPTIMIZATION SETTINGS ---

# Reduce delay between requests (default: 0)
DOWNLOAD_DELAY = 0  # No delay for maximum speed

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# AutoThrottle - DISABLED for maximum speed
AUTOTHROTTLE_ENABLED = False

# --- CONCURRENCY SETTINGS FOR MAXIMUM PERFORMANCE ---
# These are aggressive settings for maximum speed
CONCURRENT_REQUESTS = 100  # Total concurrent requests
CONCURRENT_REQUESTS_PER_DOMAIN = 100  # Concurrent requests per domain
CONCURRENT_ITEMS = 200  # Number of concurrent items to process

# Increase threadpool size
REACTOR_THREADPOOL_MAXSIZE = 30

# DNS cache settings
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = 10000
DNS_TIMEOUT = 60

# --- RETRY AND TIMEOUT SETTINGS ---
# These settings are crucial for working with a large, unreliable proxy pool.

# Enable the Retry middleware
RETRY_ENABLED = True

# Number of times to retry a failed request. We set this high because many
# free proxies will fail before we find a working one.
RETRY_TIMES = 100

# A lower timeout helps to discard bad proxies faster.
DOWNLOAD_TIMEOUT = 20

from twisted.internet.defer import TimeoutError as DeferTimeoutError
from twisted.internet.error import (
    TimeoutError as NetTimeoutError,
    DNSLookupError,
    ConnectionRefusedError,
    ConnectionDone,
    ConnectError,
    ConnectionLost,
    TCPTimedOutError,
)
from twisted.web.client import ResponseFailed
from middlewares import BotDetectionError

# Add our custom exception and the default Twisted exceptions to the retry list
RETRY_EXCEPTIONS = [
    DeferTimeoutError,
    NetTimeoutError,
    DNSLookupError,
    ConnectionRefusedError,
    ConnectionDone,
    ConnectError,
    ConnectionLost,
    TCPTimedOutError,
    ResponseFailed,
    OSError,
    BotDetectionError,  # Retry on our custom bot detection error
]

# Custom setting for the unified middleware
MAX_PROXY_FAILURES = 3

# --- END RETRY AND TIMEOUT SETTINGS ---

# --- DOWNLOADER MIDDLEWARES ---
# The unified middleware handles both proxy rotation and browser requests.
# Priority 543 is before RetryMiddleware (550) to ensure we handle responses first
DOWNLOADER_MIDDLEWARES = {
   "enhanced_middleware.EnhancedProxyBrowserMiddleware": 543,
   # Ensure RetryMiddleware is enabled with default priority
   "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
}

# --- MEMORY OPTIMIZATION ---
# Limit the size of the request queue
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.FifoMemoryQueue'

# --- LOG SETTINGS FOR PERFORMANCE ---
# Reduce logging overhead in production
LOG_LEVEL = 'INFO'  # Change to 'WARNING' or 'ERROR' for even better performance