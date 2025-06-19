import re
import scrapy
import json
import os
from typing import Set, Dict, Any
from datetime import datetime
import socket
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# List of publicly available proxy list sources
PROXY_SOURCES = [
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
    "https://raw.githubusercontent.com/clash/proxy-list/main/https.txt",
    "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://free-proxy-list.net/",
    "https://spys.me/proxy.txt",
    "https://proxyscrape.com/free-proxy-list",
]

# Residential proxy sources (if available)
RESIDENTIAL_SOURCES = [
    # Add residential proxy sources here if you find any free ones
]

IP_PORT_REGEX = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}:\d+\b")


class ProxyQualityChecker:
    """Helper class to check proxy quality and determine proxy type."""
    
    # Known datacenter IP ranges (partial list)
    DATACENTER_RANGES = [
        "104.16.", "104.17.", "104.18.", "104.19.",  # Cloudflare
        "172.64.", "172.65.", "172.66.", "172.67.",  # Cloudflare
        "162.158.", "162.159.",  # Cloudflare
        "141.101.",  # Cloudflare
        "108.162.",  # Cloudflare
        "173.245.", "188.114.", "190.93.", "197.234.", "198.41.",  # Cloudflare
        "13.", "52.", "54.",  # AWS
        "35.", "34.",  # Google Cloud
        "20.", "40.", "104.40.", "104.41.", "104.42.", "104.43.", "104.44.", "104.45.",  # Azure
        "45.", "47.",  # Various datacenters
        "185.", "194.", "195.",  # European datacenters
        "5.", "31.", "37.", "46.", "78.", "79.", "80.", "81.", "82.", "83.", "84.", "85.", "86.", "87.", "88.", "89.", "91.", "92.", "93.", "94.", "95.",  # Various hosting
    ]
    
    @staticmethod
    def is_datacenter_ip(ip: str) -> bool:
        """Check if IP appears to be from a datacenter."""
        for prefix in ProxyQualityChecker.DATACENTER_RANGES:
            if ip.startswith(prefix):
                return True
        return False
    
    @staticmethod
    def check_port_quality(port: int) -> int:
        """Score based on port number - common proxy ports score lower."""
        common_proxy_ports = [3128, 8080, 8888, 8118, 1080, 9050, 4145]
        if port in common_proxy_ports:
            return 0  # Common proxy port, likely overused
        elif port > 10000:
            return 2  # High port, potentially less used
        else:
            return 1  # Standard port
    
    @staticmethod
    def detect_proxy_type(ip: str, port: int, source_url: str = "") -> Dict[str, Any]:
        """Detect proxy type and calculate quality score."""
        proxy_info = {
            "type": "datacenter",  # Default
            "protocol": "http",  # Default
            "quality_score": 0,
            "is_residential": False,
            "is_datacenter": True,
            "port_score": 0,
            "source_score": 0
        }
        
        # Check if it's a datacenter IP
        is_dc = ProxyQualityChecker.is_datacenter_ip(ip)
        proxy_info["is_datacenter"] = is_dc
        proxy_info["is_residential"] = not is_dc
        
        # Determine type
        if not is_dc:
            proxy_info["type"] = "residential"
            proxy_info["quality_score"] += 10  # Residential proxies get high base score
        
        # Check port quality
        port_score = ProxyQualityChecker.check_port_quality(port)
        proxy_info["port_score"] = port_score
        proxy_info["quality_score"] += port_score
        
        # Source quality scoring
        if "github" in source_url.lower():
            proxy_info["source_score"] = 1
        elif any(premium in source_url.lower() for premium in ["spys", "proxy-list"]):
            proxy_info["source_score"] = 2
        else:
            proxy_info["source_score"] = 0
        proxy_info["quality_score"] += proxy_info["source_score"]
        
        # Protocol detection based on port
        if port in [443, 8443]:
            proxy_info["protocol"] = "https"
            proxy_info["quality_score"] += 1
        elif port in [1080, 4145, 5555]:
            proxy_info["protocol"] = "socks"
            proxy_info["quality_score"] -= 2  # SOCKS doesn't work with Selenium
        
        return proxy_info
    
    @staticmethod
    def quick_connectivity_test(ip: str, port: int, timeout: int = 2) -> bool:
        """Quick socket connection test."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((ip, port))
            sock.close()
            return result == 0
        except:
            return False


class FreeProxySpider(scrapy.Spider):
    """Scrapes several free-proxy index pages and yields IP / port pairs with quality scoring."""

    name = "free_proxy_spider"
    start_urls = PROXY_SOURCES + RESIDENTIAL_SOURCES

    custom_settings = {
        "DOWNLOAD_TIMEOUT": 15,
        "CONCURRENT_REQUESTS": 8,
        "DOWNLOAD_DELAY": 0.5,
        "LOG_LEVEL": "INFO",
        # Speed over reliability – failures are acceptable.
        "RETRY_ENABLED": False,
        "REDIRECT_ENABLED": False,
        # Don't use feeds, we'll handle JSON output manually
        "FEEDS": {},
    }

    def __init__(self, test_connectivity=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collected_proxies = []
        self.test_connectivity = test_connectivity
        self.quality_checker = ProxyQualityChecker()
        
    def parse(self, response):
        self.logger.info(f"Parsing proxies from: {response.url}")
        proxies: Set[str] = set(IP_PORT_REGEX.findall(response.text))
        
        # Check if this is a residential source
        is_residential_source = response.url in RESIDENTIAL_SOURCES
        
        for hostport in proxies:
            try:
                ip, port = hostport.split(":")
                port = int(port)
                
                # Get proxy quality information
                proxy_quality = ProxyQualityChecker.detect_proxy_type(ip, port, response.url)
                
                # Override type if from residential source
                if is_residential_source:
                    proxy_quality["type"] = "residential"
                    proxy_quality["is_residential"] = True
                    proxy_quality["is_datacenter"] = False
                    proxy_quality["quality_score"] += 10
                
                proxy_data = {
                    "ip": ip,
                    "port": port,
                    "protocol": proxy_quality["protocol"],
                    "proxy": f"{proxy_quality['protocol']}://{ip}:{port}",
                    "source": response.url,
                    "collected_at": datetime.utcnow().isoformat(),
                    "type": proxy_quality["type"],
                    "quality_score": proxy_quality["quality_score"],
                    "is_residential": proxy_quality["is_residential"],
                    "is_datacenter": proxy_quality["is_datacenter"],
                    "metadata": {
                        "port_score": proxy_quality["port_score"],
                        "source_score": proxy_quality["source_score"],
                        "detected_protocol": proxy_quality["protocol"]
                    }
                }
                
                self.collected_proxies.append(proxy_data)
                yield proxy_data
            except ValueError:
                # Skip malformed proxy entries
                continue
        
        self.logger.info(f"Found {len(proxies)} proxies from {response.url}")
    
    def test_proxy_batch(self, proxies: list, max_workers: int = 50) -> Dict[str, bool]:
        """Test a batch of proxies for connectivity."""
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {
                executor.submit(
                    ProxyQualityChecker.quick_connectivity_test, 
                    proxy["ip"], 
                    proxy["port"]
                ): f"{proxy['ip']}:{proxy['port']}"
                for proxy in proxies
            }
            
            for future in as_completed(future_to_proxy):
                proxy_key = future_to_proxy[future]
                try:
                    results[proxy_key] = future.result()
                except Exception:
                    results[proxy_key] = False
                    
        return results
    
    def closed(self, reason):
        """Called when spider closes - save all proxies to JSON file with quality scores."""
        if self.collected_proxies:
            # Remove duplicates based on IP:PORT combination
            unique_proxies = {}
            for proxy in self.collected_proxies:
                key = f"{proxy['ip']}:{proxy['port']}"
                if key not in unique_proxies or proxy["quality_score"] > unique_proxies[key]["quality_score"]:
                    unique_proxies[key] = proxy
            
            unique_proxy_list = list(unique_proxies.values())
            
            # Optional: Test connectivity
            if self.test_connectivity:
                self.logger.info("Testing proxy connectivity...")
                test_results = self.test_proxy_batch(unique_proxy_list)
                
                # Update proxies with connectivity status
                for proxy in unique_proxy_list:
                    key = f"{proxy['ip']}:{proxy['port']}"
                    proxy["is_alive"] = test_results.get(key, False)
                    if proxy["is_alive"]:
                        proxy["quality_score"] += 5  # Bonus for being alive
            
            # Sort by quality score (highest first)
            unique_proxy_list.sort(key=lambda x: x["quality_score"], reverse=True)
            
            # Separate by type
            residential_proxies = [p for p in unique_proxy_list if p["is_residential"]]
            datacenter_proxies = [p for p in unique_proxy_list if p["is_datacenter"]]
            socks_proxies = [p for p in unique_proxy_list if p["protocol"] in ["socks", "socks4", "socks5"]]
            
            # Ensure helpers directory exists
            os.makedirs("helpers", exist_ok=True)
            
            # Create JSON structure compatible with ProxyManager
            proxy_json = {
                "metadata": {
                    "source": "free_proxy_spider",
                    "collected_at": datetime.utcnow().isoformat(),
                    "total_count": len(unique_proxy_list),
                    "residential_count": len(residential_proxies),
                    "datacenter_count": len(datacenter_proxies),
                    "socks_count": len(socks_proxies),
                    "tested_connectivity": self.test_connectivity,
                    "note": "Proxies are sorted by quality score. Residential proxies have highest priority."
                },
                "proxies": unique_proxy_list,
                "summary": {
                    "by_type": {
                        "residential": len(residential_proxies),
                        "datacenter": len(datacenter_proxies)
                    },
                    "by_protocol": {
                        "http": len([p for p in unique_proxy_list if p["protocol"] == "http"]),
                        "https": len([p for p in unique_proxy_list if p["protocol"] == "https"]),
                        "socks": len(socks_proxies)
                    },
                    "quality_distribution": {
                        "high_quality": len([p for p in unique_proxy_list if p["quality_score"] >= 10]),
                        "medium_quality": len([p for p in unique_proxy_list if 5 <= p["quality_score"] < 10]),
                        "low_quality": len([p for p in unique_proxy_list if p["quality_score"] < 5])
                    }
                }
            }
            
            # Save to helpers/proxies.json
            output_file = "helpers/proxies.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(proxy_json, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved {len(unique_proxy_list)} unique proxies to {output_file}")
            print(f"\n🎉 Successfully collected {len(unique_proxy_list)} unique proxies!")
            print(f"📊 Quality Distribution:")
            print(f"   - 🏠 Residential: {len(residential_proxies)}")
            print(f"   - 🏢 Datacenter: {len(datacenter_proxies)}")
            print(f"   - 🔌 SOCKS (not supported): {len(socks_proxies)}")
            print(f"   - ⭐ High Quality (score ≥ 10): {proxy_json['summary']['quality_distribution']['high_quality']}")
            print(f"   - 📈 Medium Quality (5-9): {proxy_json['summary']['quality_distribution']['medium_quality']}")
            print(f"   - 📉 Low Quality (< 5): {proxy_json['summary']['quality_distribution']['low_quality']}")
            print(f"📁 Saved to: {output_file}")
            print(f"🔄 These proxies will now be used by your scraper automatically.")
            
            if residential_proxies:
                print(f"\n✨ Great news! Found {len(residential_proxies)} potential residential proxies!")
            else:
                print(f"\n⚠️  No residential proxies found. Consider getting residential proxies for better success rates.")
        else:
            self.logger.warning("No proxies were collected!")
            print("❌ No proxies were collected. Check the sources and try again.") 