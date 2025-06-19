# Walmart Retail Scraper

A comprehensive, high-performance web scraping solution for Walmart product data, store information, and category analysis. Built with Scrapy and featuring advanced proxy management, browser automation, and anti-detection capabilities.

## 🚀 Features

- **Multi-Spider Architecture**: Store locations, product data, categories, and search functionality
- **Advanced Proxy Management**: Intelligent proxy rotation with quality scoring and validation
- **Browser Automation**: Undetected Chrome browser support for complex JavaScript pages
- **Anti-Detection**: Human-like behavior simulation, fingerprint randomization, and session persistence
- **Parallel Processing**: High-performance concurrent scraping with intelligent rate limiting
- **Data Export**: JSON, CSV, and real-time streaming output options
- **Discord Integration**: Real-time progress monitoring and notifications
- **Robust Error Handling**: Automatic retries, proxy rotation, and graceful failure recovery

## 📋 Requirements

- Python 3.8+
- Chrome/Chromium browser
- 4GB+ RAM (recommended for browser automation)
- Stable internet connection

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd retailScraper
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # Mac/Linux
   source .venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Install Chrome dependencies** (if using browser automation):
   - Ensure Chrome/Chromium is installed
   - ChromeDriver will be automatically managed by undetected-chromedriver

## 🎯 Quick Start

### 1. Get Free Proxies (Optional but Recommended)
```bash
# Collect free proxies
scrapy crawl free_proxy_spider

# Validate proxies specifically for Walmart
python scripts/validate_proxies_walmart.py --limit 50
```

### 2. Run Basic Store Scraper
```bash
# Scrape Walmart store locations
scrapy crawl walmart_stores_spider -L INFO

# With enhanced settings for better proxy performance
scrapy crawl walmart_stores_spider -L INFO --set SCRAPY_SETTINGS_MODULE=scrapy_settings_enhanced
```

### 3. Test Your Setup
```bash
# Quick test to verify improvements
python scripts/test_improvements.py

# Diagnose specific proxy
python scripts/diagnose_single_proxy.py --proxy "http://1.2.3.4:8080"
```

## 🕷️ Available Spiders

| Spider | Description | Output |
|--------|-------------|--------|
| `walmart_stores_spider` | Store locations, hours, services | `data/stores.json` |
| `walmart_products_spider` | Product details by store | `data/products.json` |
| `walmart_products_parallel_spider` | High-speed parallel product scraping | `data/products.json` |
| `walmart_categories_spider` | Category hierarchy and structure | `data/categories.json` |
| `free_proxy_spider` | Free proxy collection and validation | `helpers/proxies.json` |

## ⚙️ Configuration

### Basic Settings (`scrapy_settings.py`)
```python
# Performance settings
CONCURRENT_REQUESTS = 100
DOWNLOAD_DELAY = 0
DOWNLOAD_TIMEOUT = 20

# Proxy settings
RETRY_TIMES = 100
MAX_PROXY_FAILURES = 3

# Browser pool
BROWSER_POOL_SIZE = 50
```

### Enhanced Settings (`scrapy_settings_enhanced.py`)
For better proxy performance with anti-detection features:
```bash
scrapy crawl <spider> --set SCRAPY_SETTINGS_MODULE=scrapy_settings_enhanced
```

### Discord Notifications (Optional)
1. Create `helpers/discord_config.py`:
   ```python
   DISCORD_WEBHOOK_URL = "your_webhook_url_here"
   ```
2. Spiders will automatically send progress updates

## 🔧 Proxy Management

### Quality-Based Proxy Selection
The system automatically prioritizes proxies based on:
- **Walmart-specific testing**: Actual success rates against Walmart
- **Geographic location**: US-based proxies preferred
- **Response times**: Faster proxies ranked higher
- **Success patterns**: Learning from historical performance

### Proxy Sources
```bash
# Collect new proxies
scrapy crawl free_proxy_spider

# Test existing proxies
python scripts/validate_proxies_walmart.py

# Manual proxy testing
python scripts/diagnose_single_proxy.py --proxy "http://ip:port"
```

### Adaptive Features
- **Cooldown periods**: Failed proxies get temporary bans
- **Subnet diversity**: Avoids using multiple proxies from same network
- **Session persistence**: Maintains cookies and state per proxy
- **Success pattern learning**: Adapts timing and behavior based on what works

## 🎭 Anti-Detection Features

### Browser Fingerprinting Protection
- **User Agent Rotation**: 50+ realistic browser fingerprints
- **Timezone Matching**: Browser timezone matches proxy location
- **Canvas/WebGL Protection**: Randomized fingerprints
- **WebRTC Blocking**: Prevents IP leaks

### Human-like Behavior
- **Natural Navigation**: Referrer headers and browsing patterns
- **Variable Timing**: Randomized delays and request intervals
- **Mouse Simulation**: Realistic cursor movements and scrolling
- **Session Warming**: Gradual trust building with target site

### Request Patterns
- **Referrer Simulation**: 70% requests appear from Google/social media
- **Header Variation**: Dynamic headers matching browser profiles
- **Cookie Management**: Persistent sessions with proper cookie handling

## 📊 Monitoring & Debugging

### Real-time Statistics
```bash
# View proxy performance
python -c "
from helpers.adaptive_proxy_manager import AdaptiveProxyManager
pm = AdaptiveProxyManager()
print(pm.get_stats_summary())
"
```

### Log Analysis
```bash
# Monitor success rates
grep "SUCCESS:" logs/scrapy.log | wc -l

# Check for bot detection
grep -i "robot\|blocked" logs/scrapy.log

# Proxy performance
grep "Browser Pool" logs/scrapy.log
```

### Discord Integration
Enable real-time monitoring:
1. Set up Discord webhook
2. Configure `helpers/discord_config.py`
3. Receive automated progress updates

## 🔍 Troubleshooting

### Common Issues

**High Failure Rates (>80%)**:
```bash
# Reduce concurrency
--set CONCURRENT_REQUESTS=10

# Increase delays
--set DOWNLOAD_DELAY=5

# Use enhanced settings
--set SCRAPY_SETTINGS_MODULE=scrapy_settings_enhanced
```

**Bot Detection Issues**:
```bash
# Test proxy quality
python scripts/validate_proxies_walmart.py

# Enable headed browser for debugging
python scripts/diagnose_single_proxy.py --proxy "http://ip:port" --headed
```

**Memory Issues**:
```bash
# Reduce browser pool
--set BROWSER_POOL_SIZE=20

# Clear temp directories
python -c "from helpers.helpers import cleanup_temp_directories; cleanup_temp_directories()"
```

### Performance Optimization

**For Free Proxies**:
- Start with 5-10 concurrent requests
- Use delays of 3-5 seconds
- Focus on US-based proxies
- Enable all anti-detection features

**For Paid Proxies**:
- Can use 50-100 concurrent requests
- Reduce delays to 1-2 seconds
- Geographic targeting available

## 📁 Project Structure

```
retailScraper/
├── crawlers/                 # Spider implementations
│   ├── walmart_stores_spider.py
│   ├── walmart_products_spider.py
│   ├── free_proxy_spider.py
│   └── ...
├── helpers/                  # Utility modules
│   ├── helpers.py           # Core utilities
│   ├── adaptive_proxy_manager.py
│   ├── config.py
│   └── discord_config.py
├── scripts/                  # Standalone tools
│   ├── validate_proxies_walmart.py
│   ├── diagnose_single_proxy.py
│   ├── apply_immediate_improvements.py
│   └── test_improvements.py
├── data/                     # Output directory
├── temp/                     # Browser sessions
├── middlewares.py           # Scrapy middleware
├── pipelines.py            # Data processing
├── scrapy_settings.py      # Basic configuration
├── scrapy_settings_enhanced.py  # Anti-detection config
└── main.py                 # Main entry point
```

## 🆕 Recent Improvements

The project includes significant enhancements for working with free proxies:

### Applied Automatically
- ✅ **Cookies Enabled**: Session persistence for better success rates
- ✅ **Expanded User Agents**: 50+ realistic browser fingerprints
- ✅ **Referrer Headers**: Natural navigation patterns
- ✅ **Enhanced Settings**: Optimized configuration for free proxies

### Advanced Features Available
- 🔧 **Walmart-specific Proxy Validation**: Test proxies against actual target
- 🤖 **Adaptive Proxy Management**: Learning system for proxy performance
- 🎭 **Advanced Anti-detection**: Fingerprint protection and human simulation
- 📊 **Performance Monitoring**: Real-time statistics and optimization

## 📈 Performance Expectations

### Free Proxies
- **Before improvements**: 5-10% success rate
- **After improvements**: 20-40% success rate
- **Recommended settings**: Low concurrency, high delays

### Paid Residential Proxies
- **Expected success rate**: 80-95%
- **Recommended settings**: Medium-high concurrency, moderate delays

### Data Collection Rates
- **Store data**: 1,000-5,000 stores/hour (depending on proxy quality)
- **Product data**: 500-2,000 products/hour
- **Category mapping**: Complete hierarchy in 1-2 hours

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make improvements
4. Test thoroughly
5. Submit pull request

## ⚠️ Legal Disclaimer

This tool is for educational and research purposes only. Users are responsible for:
- Complying with website terms of service
- Respecting robots.txt files
- Following applicable laws and regulations
- Using appropriate rate limiting
- Not overloading target servers

## 📝 License

[Specify your license here]

## 🆘 Support

- Check the troubleshooting section above
- Review logs for specific error messages
- Test with single proxy using diagnostic tools
- Ensure proper configuration for your use case

---

**Note**: For production use with commercial applications, consider investing in high-quality residential proxy services for better reliability and success rates. 