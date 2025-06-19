import json
import os
import time
import random
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import pickle
import logging

logger = logging.getLogger(__name__)

class EnhancedProxyManager:
    """
    Enhanced proxy manager specifically optimized for Walmart scraping.
    Prioritizes residential proxies and filters out easily detected datacenter proxies.
    """
    
    def __init__(self, proxy_file: str = "helpers/walmart_validated_proxies.json", 
                 fallback_file: str = "helpers/proxies.json"):
        self.primary_file = proxy_file
        self.fallback_file = fallback_file
        self.lock = threading.Lock()
        
        # Proxy categorization
        self.residential_proxies = []
        self.mobile_proxies = []
        self.datacenter_proxies = []
        self.all_proxies = []
        
        # Performance tracking
        self.proxy_stats = defaultdict(lambda: {
            "requests": 0,
            "successes": 0,
            "failures": 0,
            "bot_detections": 0,
            "last_used": None,
            "last_success": None,
            "last_failure": None,
            "avg_response_time": 0,
            "cooldown_until": None,
            "consecutive_failures": 0,
            "walmart_score": 0
        })
        
        # Load and categorize proxies
        self._load_and_categorize_proxies()
        
        # Walmart-specific tracking
        self.walmart_blocked_ips = set()
        self.session_success_patterns = []
        
    def _load_and_categorize_proxies(self):
        """Load proxies and categorize them by type"""
        logger.info("Loading and categorizing proxies...")
        
        # Try walmart-validated proxies first
        if os.path.exists(self.primary_file):
            try:
                with open(self.primary_file, "r") as f:
                    data = json.load(f)
                    if "proxies" in data:
                        for proxy in data["proxies"]:
                            self._categorize_proxy(proxy)
                        logger.info(f"Loaded {len(data['proxies'])} Walmart-validated proxies")
            except Exception as e:
                logger.error(f"Error loading Walmart-validated proxies: {e}")
        
        # Fallback to general proxies if needed
        if not self.all_proxies and os.path.exists(self.fallback_file):
            try:
                with open(self.fallback_file, "r") as f:
                    data = json.load(f)
                    proxies = data if isinstance(data, list) else data.get("proxies", [])
                    
                    for proxy in proxies:
                        self._categorize_proxy(proxy)
                    
                    logger.info(f"Loaded {len(proxies)} fallback proxies")
            except Exception as e:
                logger.error(f"Error loading fallback proxies: {e}")
        
        # Prioritize residential and mobile proxies
        self._apply_walmart_filters()
        
        logger.info(f"Proxy summary: {len(self.residential_proxies)} residential, "
                   f"{len(self.mobile_proxies)} mobile, {len(self.datacenter_proxies)} datacenter")
    
    def _categorize_proxy(self, proxy_data: Dict):
        """Categorize a proxy based on its properties"""
        # Extract proxy URL
        if isinstance(proxy_data, str):
            proxy_url = proxy_data
            proxy_info = {"proxy": proxy_url}
        else:
            proxy_url = proxy_data.get("proxy") or f"http://{proxy_data.get('ip')}:{proxy_data.get('port')}"
            proxy_info = proxy_data.copy()
            proxy_info["proxy"] = proxy_url
        
        # Skip SOCKS proxies - they don't work with Selenium
        protocol = proxy_info.get("protocol", "http").lower()
        if protocol in ["socks", "socks4", "socks5"]:
            logger.debug(f"Skipping SOCKS proxy: {proxy_url}")
            return
        
        # Categorize by type
        location = proxy_info.get("location", {})
        proxy_type = proxy_info.get("type", "").lower()
        isp = location.get("isp", "").lower() if isinstance(location, dict) else ""
        
        # Check if residential
        if (proxy_type == "residential" or 
            proxy_info.get("is_residential", False) or
            any(res_isp in isp for res_isp in ["comcast", "verizon", "at&t", "spectrum", "cox", "charter", "centurylink"])):
            self.residential_proxies.append(proxy_info)
            proxy_info["category"] = "residential"
        # Check if mobile
        elif (proxy_type == "mobile" or 
              any(mobile_isp in isp for mobile_isp in ["t-mobile", "sprint", "vodafone", "orange"])):
            self.mobile_proxies.append(proxy_info)
            proxy_info["category"] = "mobile"
        else:
            self.datacenter_proxies.append(proxy_info)
            proxy_info["category"] = "datacenter"
        
        self.all_proxies.append(proxy_info)
        
        # Initialize stats with Walmart score if available
        if proxy_info.get("walmart_score"):
            self.proxy_stats[proxy_url]["walmart_score"] = proxy_info["walmart_score"]
    
    def _apply_walmart_filters(self):
        """Apply Walmart-specific filters to prioritize best proxies"""
        # Filter out known bad datacenter providers
        bad_providers = ["digitalocean", "linode", "vultr", "ovh", "hetzner", "aws", "google", "azure"]
        
        filtered_datacenter = []
        for proxy in self.datacenter_proxies:
            location = proxy.get("location", {})
            isp = location.get("isp", "").lower() if isinstance(location, dict) else ""
            org = location.get("org", "").lower() if isinstance(location, dict) else ""
            
            # Skip known bad providers
            if any(provider in isp or provider in org for provider in bad_providers):
                continue
                
            filtered_datacenter.append(proxy)
        
        self.datacenter_proxies = filtered_datacenter
        
        # Prioritize US-based proxies for Walmart
        for proxies in [self.residential_proxies, self.mobile_proxies, self.datacenter_proxies]:
            us_proxies = [p for p in proxies if p.get("location", {}).get("countryCode") == "US"]
            other_proxies = [p for p in proxies if p.get("location", {}).get("countryCode") != "US"]
            proxies[:] = us_proxies + other_proxies
    
    def get_proxy(self, request_context: Optional[Dict] = None) -> Optional[str]:
        """Get the best available proxy for Walmart scraping"""
        with self.lock:
            current_time = datetime.now()
            
            # Build list of available proxies with scores
            available_proxies = []
            
            # Prioritize proxy types for Walmart
            proxy_pools = [
                (self.residential_proxies, 100),  # Highest priority
                (self.mobile_proxies, 80),         # High priority
                (self.datacenter_proxies, 30)     # Lower priority
            ]
            
            for proxy_list, base_score in proxy_pools:
                for proxy_info in proxy_list:
                    proxy_url = proxy_info["proxy"]
                    stats = self.proxy_stats[proxy_url]
                    
                    # Skip if blocked by Walmart
                    if proxy_url in self.walmart_blocked_ips:
                        continue
                    
                    # Skip if in cooldown
                    if stats["cooldown_until"] and current_time < stats["cooldown_until"]:
                        continue
                    
                    # Skip if too many consecutive failures
                    if stats["consecutive_failures"] >= 3:
                        continue
                    
                    # Calculate dynamic score
                    score = base_score + stats.get("walmart_score", 0)
                    
                    # Adjust score based on performance
                    if stats["requests"] > 0:
                        success_rate = stats["successes"] / stats["requests"]
                        score += success_rate * 50
                        
                        # Heavy penalty for bot detections
                        if stats["bot_detections"] > 0:
                            detection_rate = stats["bot_detections"] / stats["requests"]
                            score -= detection_rate * 100
                    
                    # Boost for recent success
                    if stats["last_success"]:
                        minutes_since_success = (current_time - stats["last_success"]).total_seconds() / 60
                        if minutes_since_success < 5:
                            score += 20
                        elif minutes_since_success < 30:
                            score += 10
                    
                    # US proxy bonus for Walmart
                    if proxy_info.get("location", {}).get("countryCode") == "US":
                        score += 15
                    
                    # Low latency bonus
                    if proxy_info.get("latency_ms", float('inf')) < 1000:
                        score += 10
                    
                    available_proxies.append((proxy_url, score, proxy_info))
            
            if not available_proxies:
                logger.warning("No available proxies! Resetting all cooldowns...")
                # Reset all proxies as last resort
                for proxy_url in self.proxy_stats:
                    self.proxy_stats[proxy_url]["cooldown_until"] = None
                    self.proxy_stats[proxy_url]["consecutive_failures"] = 0
                # Try again
                return self.get_proxy(request_context)
            
            # Sort by score and select
            available_proxies.sort(key=lambda x: x[1], reverse=True)
            
            # Log top proxies for debugging
            if len(available_proxies) > 0:
                logger.debug(f"Top 3 available proxies: {[(p[0], p[1], p[2]['category']) for p in available_proxies[:3]]}")
            
            # Select proxy (sometimes randomize from top performers)
            if random.random() < 0.2 and len(available_proxies) > 3:
                selected = random.choice(available_proxies[:3])
            else:
                selected = available_proxies[0]
            
            proxy_url, score, proxy_info = selected
            
            # Update stats
            self.proxy_stats[proxy_url]["last_used"] = current_time
            self.proxy_stats[proxy_url]["requests"] += 1
            
            logger.info(f"Selected {proxy_info['category']} proxy: {proxy_url} (score: {score:.1f})")
            
            return proxy_url
    
    def record_success(self, proxy: str, response_time: Optional[float] = None):
        """Record successful request"""
        with self.lock:
            stats = self.proxy_stats[proxy]
            current_time = datetime.now()
            
            stats["successes"] += 1
            stats["last_success"] = current_time
            stats["consecutive_failures"] = 0
            stats["cooldown_until"] = None
            
            # Update response time
            if response_time:
                if stats["avg_response_time"] == 0:
                    stats["avg_response_time"] = response_time
                else:
                    stats["avg_response_time"] = (stats["avg_response_time"] + response_time) / 2
            
            # Increase Walmart score
            stats["walmart_score"] = min(100, stats["walmart_score"] + 5)
            
            logger.debug(f"Proxy success: {proxy} (Walmart score: {stats['walmart_score']})")
    
    def record_failure(self, proxy: str, is_bot_detection: bool = False):
        """Record failed request"""
        with self.lock:
            stats = self.proxy_stats[proxy]
            current_time = datetime.now()
            
            stats["failures"] += 1
            stats["last_failure"] = current_time
            stats["consecutive_failures"] += 1
            
            if is_bot_detection:
                stats["bot_detections"] += 1
                # Severe penalty for bot detection
                stats["walmart_score"] = max(-50, stats["walmart_score"] - 20)
                
                # Long cooldown for bot detection
                stats["cooldown_until"] = current_time + timedelta(minutes=30)
                
                # Mark as Walmart-blocked if too many detections
                if stats["bot_detections"] >= 3:
                    self.walmart_blocked_ips.add(proxy)
                    logger.warning(f"Proxy permanently blocked by Walmart: {proxy}")
            else:
                # Regular failure
                stats["walmart_score"] = max(-20, stats["walmart_score"] - 5)
                
                # Progressive cooldown based on consecutive failures
                if stats["consecutive_failures"] >= 3:
                    stats["cooldown_until"] = current_time + timedelta(minutes=15)
                elif stats["consecutive_failures"] >= 2:
                    stats["cooldown_until"] = current_time + timedelta(minutes=5)
                else:
                    stats["cooldown_until"] = current_time + timedelta(minutes=1)
            
            logger.debug(f"Proxy failure: {proxy} (consecutive: {stats['consecutive_failures']}, "
                        f"bot detection: {is_bot_detection})")
    
    def get_stats_summary(self) -> Dict:
        """Get summary of proxy performance"""
        with self.lock:
            total_proxies = len(self.all_proxies)
            
            working_residential = len([
                p for p in self.residential_proxies 
                if self.proxy_stats[p["proxy"]]["successes"] > 0
            ])
            
            working_mobile = len([
                p for p in self.mobile_proxies 
                if self.proxy_stats[p["proxy"]]["successes"] > 0
            ])
            
            blocked_count = len(self.walmart_blocked_ips)
            
            total_requests = sum(s["requests"] for s in self.proxy_stats.values())
            total_successes = sum(s["successes"] for s in self.proxy_stats.values())
            total_bot_detections = sum(s["bot_detections"] for s in self.proxy_stats.values())
            
            return {
                "total_proxies": total_proxies,
                "residential_proxies": len(self.residential_proxies),
                "mobile_proxies": len(self.mobile_proxies),
                "datacenter_proxies": len(self.datacenter_proxies),
                "working_residential": working_residential,
                "working_mobile": working_mobile,
                "walmart_blocked": blocked_count,
                "total_requests": total_requests,
                "total_successes": total_successes,
                "total_bot_detections": total_bot_detections,
                "success_rate": total_successes / max(total_requests, 1),
                "detection_rate": total_bot_detections / max(total_requests, 1)
            }
    
    def save_stats(self, filepath: str = "helpers/enhanced_proxy_stats.pkl"):
        """Save statistics for persistence"""
        with self.lock:
            data = {
                "proxy_stats": dict(self.proxy_stats),
                "walmart_blocked_ips": list(self.walmart_blocked_ips),
                "session_success_patterns": self.session_success_patterns
            }
            
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "wb") as f:
                pickle.dump(data, f)
            
            logger.info(f"Saved proxy stats to {filepath}") 