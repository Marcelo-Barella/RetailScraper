import argparse
import sys
import os
import json
import time
import random
import socket
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import subprocess
from typing import Dict, List, Optional, Tuple

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from helpers.helpers import USER_AGENTS, ensure_dir

class WalmartProxyValidator:
    """Advanced proxy validator specifically for Walmart"""
    
    # Walmart-specific test URLs
    TEST_URLS = [
        "https://www.walmart.com/",
        "https://www.walmart.com/robots.txt",
        "https://www.walmart.com/ip/test/123456789",  # Non-existent product
    ]
    
    # Known Walmart bot detection signatures
    BOT_SIGNATURES = [
        "robot or human?",
        "are you a robot",
        "access denied",
        "blocked",
        "captcha",
        "challenge",
        "verify you're human",
        "unusual traffic",
        "automated requests"
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.results = []
        
    def get_proxy_location(self, proxy: str) -> Optional[Dict]:
        """Get geolocation of proxy using IP API"""
        try:
            ip = proxy.split("://")[-1].split(":")[0]
            # Use a free IP geolocation service
            response = requests.get(f"http://ip-api.com/json/{ip}", timeout=5)
            if response.status_code == 200:
                data = response.json()
                return {
                    "country": data.get("country", "Unknown"),
                    "countryCode": data.get("countryCode", "XX"),
                    "region": data.get("regionName", "Unknown"),
                    "city": data.get("city", "Unknown"),
                    "timezone": data.get("timezone", "Unknown"),
                    "isp": data.get("isp", "Unknown"),
                    "org": data.get("org", "Unknown"),
                    "as": data.get("as", "Unknown")
                }
        except:
            pass
        return None
    
    def check_webrtc_leak(self, proxy: str) -> bool:
        """Check if proxy leaks real IP via WebRTC (requires browser)"""
        # This would require selenium, for now return True
        # In production, you'd use browser to check WebRTC leaks
        return True
    
    def test_ssl_support(self, proxy: str) -> bool:
        """Test if proxy properly handles HTTPS"""
        try:
            test_url = "https://www.walmart.com/robots.txt"
            response = self.session.get(
                test_url,
                proxies={"http": proxy, "https": proxy},
                timeout=10,
                verify=True,  # Verify SSL certificates
                allow_redirects=False
            )
            return response.status_code in [200, 301, 302]
        except:
            return False
    
    def calculate_latency(self, proxy: str) -> Optional[float]:
        """Measure proxy latency to Walmart"""
        try:
            start_time = time.time()
            response = self.session.head(
                "https://www.walmart.com/",
                proxies={"http": proxy, "https": proxy},
                timeout=10,
                allow_redirects=False
            )
            latency = (time.time() - start_time) * 1000  # Convert to ms
            return latency if response.status_code in [200, 301, 302] else None
        except:
            return None
    
    def test_walmart_access(self, proxy: str) -> Tuple[bool, str, Dict]:
        """Test if proxy can access Walmart without triggering bot detection"""
        results = {
            "can_access": False,
            "bot_detected": False,
            "status_codes": [],
            "response_times": [],
            "error": None,
            "headers_quality": 0
        }
        
        # Test with random user agent
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
        
        for test_url in self.TEST_URLS:
            try:
                start_time = time.time()
                response = self.session.get(
                    test_url,
                    headers=headers,
                    proxies={"http": proxy, "https": proxy},
                    timeout=15,
                    allow_redirects=True
                )
                response_time = time.time() - start_time
                
                results["status_codes"].append(response.status_code)
                results["response_times"].append(response_time)
                
                # Check for bot detection
                response_text_lower = response.text.lower()
                for signature in self.BOT_SIGNATURES:
                    if signature in response_text_lower:
                        results["bot_detected"] = True
                        results["error"] = f"Bot detection: {signature}"
                        return False, f"Bot detected: {signature}", results
                
                # Check if we got actual Walmart content
                if response.status_code == 200:
                    if "walmart" in response_text_lower or test_url.endswith("robots.txt"):
                        results["can_access"] = True
                        
                        # Analyze response headers for quality
                        if "set-cookie" in response.headers:
                            results["headers_quality"] += 1
                        if "x-frame-options" in response.headers:
                            results["headers_quality"] += 1
                            
            except requests.exceptions.Timeout:
                results["error"] = "Timeout"
                return False, "Timeout", results
            except requests.exceptions.ProxyError:
                results["error"] = "Proxy connection failed"
                return False, "Proxy error", results
            except Exception as e:
                results["error"] = str(e)
                return False, f"Error: {str(e)}", results
            
            # Random delay between requests
            time.sleep(random.uniform(0.5, 2))
        
        if results["can_access"] and not results["bot_detected"]:
            avg_response_time = sum(results["response_times"]) / len(results["response_times"])
            return True, f"Success (avg {avg_response_time:.2f}s)", results
        
        return False, "Cannot access Walmart", results
    
    def validate_proxy(self, proxy_data: Dict) -> Dict:
        """Comprehensive validation of a single proxy"""
        proxy_url = proxy_data.get("proxy") or f"http://{proxy_data['ip']}:{proxy_data['port']}"
        
        result = {
            "proxy": proxy_url,
            "ip": proxy_data.get("ip"),
            "port": proxy_data.get("port"),
            "original_score": proxy_data.get("quality_score", 0),
            "walmart_score": 0,
            "is_working": False,
            "can_access_walmart": False,
            "bot_detected": False,
            "ssl_support": False,
            "latency_ms": None,
            "location": None,
            "webrtc_safe": True,
            "error": None,
            "tested_at": datetime.utcnow().isoformat()
        }
        
        # Test SSL support
        result["ssl_support"] = self.test_ssl_support(proxy_url)
        if result["ssl_support"]:
            result["walmart_score"] += 2
        
        # Get geolocation
        location = self.get_proxy_location(proxy_url)
        if location:
            result["location"] = location
            # US-based proxies get bonus points
            if location.get("countryCode") == "US":
                result["walmart_score"] += 5
            # Residential ISPs get bonus
            isp_lower = location.get("isp", "").lower()
            if any(residential in isp_lower for residential in ["comcast", "verizon", "at&t", "spectrum", "cox"]):
                result["walmart_score"] += 10
                result["likely_residential"] = True
        
        # Test latency
        latency = self.calculate_latency(proxy_url)
        if latency:
            result["latency_ms"] = latency
            result["is_working"] = True
            # Score based on latency
            if latency < 500:
                result["walmart_score"] += 3
            elif latency < 1000:
                result["walmart_score"] += 2
            elif latency < 2000:
                result["walmart_score"] += 1
        
        # Test Walmart access
        if result["is_working"]:
            can_access, message, test_results = self.test_walmart_access(proxy_url)
            result["can_access_walmart"] = can_access
            result["bot_detected"] = test_results.get("bot_detected", False)
            result["walmart_test_details"] = test_results
            
            if can_access:
                result["walmart_score"] += 20  # Big bonus for actually working with Walmart
                # Additional bonus for good response times
                avg_response = sum(test_results["response_times"]) / len(test_results["response_times"])
                if avg_response < 2:
                    result["walmart_score"] += 5
            else:
                result["error"] = message
                # Severe penalty for bot detection
                if result["bot_detected"]:
                    result["walmart_score"] -= 20
        
        return result
    
    def validate_batch(self, proxies: List[Dict], max_workers: int = 20) -> List[Dict]:
        """Validate a batch of proxies concurrently"""
        print(f"Validating {len(proxies)} proxies against Walmart...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {
                executor.submit(self.validate_proxy, proxy): proxy 
                for proxy in proxies
            }
            
            completed = 0
            for future in as_completed(future_to_proxy):
                try:
                    result = future.result()
                    self.results.append(result)
                    completed += 1
                    
                    # Progress update
                    if completed % 10 == 0:
                        working = len([r for r in self.results if r["can_access_walmart"]])
                        print(f"Progress: {completed}/{len(proxies)} tested, {working} working with Walmart")
                        
                except Exception as e:
                    print(f"Error validating proxy: {e}")
        
        return self.results

def main():
    parser = argparse.ArgumentParser(description="Validate proxies specifically for Walmart scraping")
    parser.add_argument("--input", default="helpers/proxies.json", help="Input proxy file")
    parser.add_argument("--output", default="helpers/walmart_validated_proxies.json", help="Output file")
    parser.add_argument("--workers", type=int, default=20, help="Number of concurrent workers")
    parser.add_argument("--limit", type=int, help="Limit number of proxies to test")
    args = parser.parse_args()
    
    # Load proxies
    try:
        with open(args.input, "r") as f:
            data = json.load(f)
            if isinstance(data, dict):
                proxies = data.get("proxies", [])
            else:
                proxies = data
    except FileNotFoundError:
        print(f"Error: Proxy file not found: {args.input}")
        sys.exit(1)
    
    if args.limit:
        proxies = proxies[:args.limit]
    
    # Validate proxies
    validator = WalmartProxyValidator()
    results = validator.validate_batch(proxies, max_workers=args.workers)
    
    # Sort by Walmart score
    results.sort(key=lambda x: x["walmart_score"], reverse=True)
    
    # Separate working and non-working
    working_proxies = [r for r in results if r["can_access_walmart"]]
    blocked_proxies = [r for r in results if r["bot_detected"]]
    failed_proxies = [r for r in results if not r["is_working"]]
    
    # Save results
    output_data = {
        "metadata": {
            "validated_at": datetime.utcnow().isoformat(),
            "total_tested": len(results),
            "working_with_walmart": len(working_proxies),
            "bot_detected": len(blocked_proxies),
            "connection_failed": len(failed_proxies),
            "validation_type": "walmart-specific"
        },
        "proxies": working_proxies,
        "all_results": results,
        "summary": {
            "by_location": {},
            "by_latency": {
                "under_500ms": len([r for r in working_proxies if r["latency_ms"] and r["latency_ms"] < 500]),
                "under_1s": len([r for r in working_proxies if r["latency_ms"] and r["latency_ms"] < 1000]),
                "under_2s": len([r for r in working_proxies if r["latency_ms"] and r["latency_ms"] < 2000]),
                "over_2s": len([r for r in working_proxies if r["latency_ms"] and r["latency_ms"] >= 2000])
            },
            "us_based": len([r for r in working_proxies if r["location"] and r["location"].get("countryCode") == "US"]),
            "likely_residential": len([r for r in working_proxies if r.get("likely_residential", False)])
        }
    }
    
    # Count by location
    for result in working_proxies:
        if result["location"]:
            country = result["location"]["country"]
            output_data["summary"]["by_location"][country] = output_data["summary"]["by_location"].get(country, 0) + 1
    
    # Save to file
    ensure_dir(os.path.dirname(args.output))
    with open(args.output, "w") as f:
        json.dump(output_data, f, indent=2)
    
    # Print summary
    print("\n" + "="*60)
    print("WALMART PROXY VALIDATION COMPLETE")
    print("="*60)
    print(f"Total proxies tested: {len(results)}")
    print(f"✅ Working with Walmart: {len(working_proxies)} ({len(working_proxies)/len(results)*100:.1f}%)")
    print(f"🤖 Bot detection triggered: {len(blocked_proxies)}")
    print(f"❌ Connection failed: {len(failed_proxies)}")
    
    if working_proxies:
        print(f"\n📍 Geographic Distribution:")
        for country, count in sorted(output_data["summary"]["by_location"].items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"   - {country}: {count}")
        
        print(f"\n⚡ Latency Distribution:")
        print(f"   - Under 500ms: {output_data['summary']['by_latency']['under_500ms']}")
        print(f"   - Under 1s: {output_data['summary']['by_latency']['under_1s']}")
        print(f"   - Under 2s: {output_data['summary']['by_latency']['under_2s']}")
        
        print(f"\n🏠 US-based proxies: {output_data['summary']['us_based']}")
        print(f"🏘️ Likely residential: {output_data['summary']['likely_residential']}")
        
        print(f"\n🌟 Top 5 proxies by Walmart score:")
        for i, proxy in enumerate(working_proxies[:5], 1):
            location = proxy["location"]["city"] + ", " + proxy["location"]["countryCode"] if proxy["location"] else "Unknown"
            print(f"   {i}. {proxy['proxy']} - Score: {proxy['walmart_score']}, Location: {location}, Latency: {proxy['latency_ms']:.0f}ms")
    
    print(f"\n💾 Results saved to: {args.output}")

if __name__ == "__main__":
    main() 