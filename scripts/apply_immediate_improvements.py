#!/usr/bin/env python
"""
Apply immediate improvements to enhance proxy performance.
This script makes quick changes that can improve success rates.
"""

import os
import sys
import json
import shutil
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

def backup_file(filepath):
    """Create a backup of a file before modifying it"""
    backup_path = f"{filepath}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(filepath, backup_path)
    print(f"✅ Backed up {filepath} to {backup_path}")
    return backup_path

def enable_cookies():
    """Enable cookies in scrapy settings"""
    settings_file = "scrapy_settings.py"
    
    if not os.path.exists(settings_file):
        print(f"❌ {settings_file} not found")
        return False
    
    backup_file(settings_file)
    
    with open(settings_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Enable cookies
    content = content.replace("COOKIES_ENABLED = False", "COOKIES_ENABLED = True")
    
    # Add cookie debug if not present
    if "COOKIES_DEBUG" not in content:
        content += "\n# Debug cookies to see what's happening\nCOOKIES_DEBUG = False\n"
    
    with open(settings_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("✅ Enabled cookies in scrapy_settings.py")
    return True

def expand_user_agents():
    """Expand the user agent list in helpers.py"""
    helpers_file = "helpers/helpers.py"
    
    if not os.path.exists(helpers_file):
        print(f"❌ {helpers_file} not found")
        return False
    
    # Additional user agents to add
    new_agents = [
        # Latest Chrome versions
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        
        # Edge variants
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.2903.112",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.2849.80",
        
        # Firefox variants
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
        
        # macOS variants
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
        
        # Linux variants
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
        
        # Mobile variants (sometimes less suspicious)
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 OPR/117.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    ]
    
    backup_file(helpers_file)
    
    with open(helpers_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find USER_AGENTS list and expand it
    import re
    match = re.search(r'USER_AGENTS = \[(.*?)\]', content, re.DOTALL)
    if match:
        existing_agents = match.group(1)
        # Add new agents if not already present
        for agent in new_agents:
            if agent not in existing_agents:
                existing_agents += f',\n    "{agent}"'
        
        new_content = content[:match.start()] + f'USER_AGENTS = [{existing_agents}]' + content[match.end():]
        
        with open(helpers_file, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print(f"✅ Expanded user agents list (added {len(new_agents)} new agents)")
        return True
    
    print("❌ Could not find USER_AGENTS list to expand")
    return False

def add_referrer_support():
    """Add referrer header support to middleware"""
    middleware_file = "middlewares.py"
    
    if not os.path.exists(middleware_file):
        print(f"❌ {middleware_file} not found")
        return False
    
    backup_file(middleware_file)
    
    with open(middleware_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Add referrer selection code after imports
    referrer_code = '''
# Referrer patterns for more natural navigation
REFERRER_PATTERNS = [
    ("https://www.google.com/", 0.4),  # 40% from Google
    ("https://www.walmart.com/", 0.3),  # 30% internal navigation
    (None, 0.2),  # 20% direct visits
    ("https://www.facebook.com/", 0.05),  # 5% from Facebook
    ("https://www.pinterest.com/", 0.05),  # 5% from Pinterest
]

def get_random_referrer():
    """Get a weighted random referrer"""
    import random
    r = random.random()
    cumulative = 0
    for referrer, weight in REFERRER_PATTERNS:
        cumulative += weight
        if r <= cumulative:
            return referrer
    return None
'''
    
    # Insert after imports if not already present
    if "REFERRER_PATTERNS" not in content:
        import_end = content.find("logger = logging.getLogger(__name__)")
        if import_end > 0:
            content = content[:import_end] + referrer_code + "\n" + content[import_end:]
            
            with open(middleware_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print("✅ Added referrer support to middleware")
            return True
    
    print("ℹ️ Referrer support already present")
    return True

def create_enhanced_settings():
    """Create an enhanced settings file with optimizations"""
    enhanced_settings = """# Enhanced Scrapy Settings for Better Proxy Performance

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
"""
    
    with open("scrapy_settings_enhanced.py", "w", encoding='utf-8') as f:
        f.write(enhanced_settings)
    
    print("✅ Created scrapy_settings_enhanced.py with optimizations")
    print("   To use: Add --set SCRAPY_SETTINGS_MODULE=scrapy_settings_enhanced to your scrapy command")
    return True

def create_simple_proxy_tester():
    """Create a simple script to test proxy improvements"""
    tester_script = '''#!/usr/bin/env python
"""Quick test to see if improvements are working"""

import scrapy
from scrapy.crawler import CrawlerProcess
import random

class QuickTestSpider(scrapy.Spider):
    name = "quick_test"
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'COOKIES_ENABLED': True,
        'CONCURRENT_REQUESTS': 5,
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
    }
    
    def start_requests(self):
        # Test URLs
        test_urls = [
            "https://www.walmart.com/",
            "https://www.walmart.com/browse/electronics/3944",
            "https://www.walmart.com/ip/test/123456",  # Non-existent product
        ]
        
        for url in test_urls:
            # Add referrer
            headers = {}
            if random.random() < 0.7:  # 70% with referrer
                headers['Referer'] = random.choice([
                    "https://www.google.com/",
                    "https://www.walmart.com/",
                ])
            
            yield scrapy.Request(
                url,
                headers=headers,
                meta={'use_undetected_browser': True},
                callback=self.parse,
                errback=self.errback
            )
    
    def parse(self, response):
        self.logger.info(f"✅ SUCCESS: {response.url} (Proxy: {response.meta.get('proxy', 'None')})")
        
        # Check for bot detection
        if "robot" in response.text.lower() or "blocked" in response.url:
            self.logger.warning(f"⚠️ Possible bot detection at {response.url}")
        else:
            self.logger.info(f"✨ Clean response from {response.url}")
            
    def errback(self, failure):
        self.logger.error(f"❌ FAILED: {failure.request.url}")

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(QuickTestSpider)
    process.start()
'''
    
    with open("scripts/test_improvements.py", "w", encoding='utf-8') as f:
        f.write(tester_script)
    
    # Make executable on Unix-like systems
    try:
        import stat
        os.chmod("scripts/test_improvements.py", os.stat("scripts/test_improvements.py").st_mode | stat.S_IEXEC)
    except:
        pass
    
    print("✅ Created scripts/test_improvements.py")
    return True

def main():
    print("🔧 Applying Immediate Improvements for Free Proxy Performance")
    print("=" * 60)
    
    improvements = [
        ("Enabling Cookies", enable_cookies),
        ("Expanding User Agents", expand_user_agents),
        ("Adding Referrer Support", add_referrer_support),
        ("Creating Enhanced Settings", create_enhanced_settings),
        ("Creating Test Script", create_simple_proxy_tester),
    ]
    
    success_count = 0
    for name, func in improvements:
        print(f"\n📌 {name}...")
        try:
            if func():
                success_count += 1
        except Exception as e:
            print(f"❌ Error in {name}: {e}")
    
    print("\n" + "=" * 60)
    print(f"✅ Applied {success_count}/{len(improvements)} improvements successfully!")
    
    if success_count > 0:
        print("\n📋 Next Steps:")
        print("1. Test improvements: python scripts/test_improvements.py")
        print("2. Validate proxies for Walmart: python scripts/validate_proxies_walmart.py --limit 20")
        print("3. Run spider with enhanced settings: scrapy crawl <spider> --set SCRAPY_SETTINGS_MODULE=scrapy_settings_enhanced")
        
        print("\n💡 Quick Tips:")
        print("- Monitor success rates in logs")
        print("- Start with lower concurrency (5-10) and increase gradually")
        print("- Focus on US-based proxies if possible")
        print("- Be patient - free proxies need slower, more human-like behavior")

if __name__ == "__main__":
    main() 