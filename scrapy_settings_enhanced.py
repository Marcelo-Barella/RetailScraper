# Enhanced Scrapy Settings for Better Proxy Performance

from scrapy_settings import *  # Import all existing settings

# Override specific settings for better performance with free proxies

# Enable cookies for session persistence
COOKIES_ENABLED = True
COOKIES_DEBUG = False  # Set to True to debug cookie issues

# Adjust retry settings for free proxies
RETRY_TIMES = 50  # Reduced from 100, but with smarter proxy rotation
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429, 403, 407]

# Add custom headers
DEFAULT_REQUEST_HEADERS.update({
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
})

# Increase timeout for slower free proxies
DOWNLOAD_TIMEOUT = 30  # Increased from 20

# Reduce concurrency if having issues
# CONCURRENT_REQUESTS = 50  # Reduced from 100 if needed

# Add random delays
RANDOMIZE_DOWNLOAD_DELAY = True
DOWNLOAD_DELAY = 2  # Base delay, will be randomized 50%-150%

# Browser pool size (match concurrency)
BROWSER_POOL_SIZE = 50  # Adjust based on your system resources

# Logging for better debugging
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'

# Track statistics
STATS_CLASS = 'scrapy.statscollectors.MemoryStatsCollector'

print("🚀 Using enhanced settings for better proxy performance")
