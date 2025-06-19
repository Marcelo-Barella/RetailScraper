import json
import os
import time
import random
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import pickle

class AdaptiveProxyManager:
    """
    Advanced proxy manager that learns from proxy performance and adapts strategies.
    Features:
    - Performance tracking per proxy
    - Adaptive cooldown periods
    - Success pattern learning
    - Subnet diversity management
    - Session persistence
    - Geolocation awareness
    """
    
    def __init__(self, proxy_file: str = "helpers/walmart_validated_proxies.json", 
                 fallback_file: str = "helpers/proxies.json"):
        self.primary_file = proxy_file
        self.fallback_file = fallback_file
        self.lock = threading.Lock()
        
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
            "session_data": {},
            "success_patterns": []
        })
        
        # Load proxies
        self._load_proxies()
        
        # Track subnet usage for diversity
        self.subnet_usage = defaultdict(int)
        self.last_subnet_reset = datetime.now()
        
        # Success pattern tracking
        self.global_patterns = {
            "successful_user_agents": defaultdict(int),
            "successful_times": defaultdict(int),  # Hour of day
            "successful_request_intervals": [],
            "successful_session_lengths": []
        }
        
        # Load persistent stats if available
        self._load_stats()
        
    def _load_proxies(self):
        """Load proxies from validated file first, then fallback"""
        self.proxies = []
        self.proxy_details = {}
        
        # Try primary file first (Walmart-validated proxies)
        try:
            with open(self.primary_file, "r") as f:
                data = json.load(f)
                if isinstance(data, dict) and "proxies" in data:
                    for proxy in data["proxies"]:
                        proxy_url = proxy.get("proxy")
                        if proxy_url:
                            self.proxies.append(proxy_url)
                            self.proxy_details[proxy_url] = proxy
                            # Initialize with Walmart validation data
                            if proxy.get("walmart_score"):
                                self.proxy_stats[proxy_url]["base_score"] = proxy["walmart_score"]
            print(f"Loaded {len(self.proxies)} Walmart-validated proxies")
        except:
            print(f"Could not load Walmart-validated proxies from {self.primary_file}")
        
        # If no proxies loaded, try fallback
        if not self.proxies:
            try:
                with open(self.fallback_file, "r") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        proxy_list = data.get("proxies", [])
                    else:
                        proxy_list = data
                    
                    for proxy in proxy_list:
                        if isinstance(proxy, dict):
                            proxy_url = proxy.get("proxy") or f"http://{proxy['ip']}:{proxy['port']}"
                            self.proxies.append(proxy_url)
                            self.proxy_details[proxy_url] = proxy
                        else:
                            self.proxies.append(proxy)
                            self.proxy_details[proxy] = {"proxy": proxy}
                
                print(f"Loaded {len(self.proxies)} proxies from fallback file")
            except:
                print("Could not load any proxies")
    
    def _get_subnet(self, proxy: str) -> str:
        """Extract subnet from proxy IP"""
        try:
            ip = proxy.split("://")[-1].split(":")[0]
            parts = ip.split(".")
            return f"{parts[0]}.{parts[1]}.{parts[2]}"
        except:
            return "unknown"
    
    def _calculate_dynamic_score(self, proxy: str) -> float:
        """Calculate dynamic score based on recent performance"""
        stats = self.proxy_stats[proxy]
        details = self.proxy_details.get(proxy, {})
        
        # Base score from validation
        score = stats.get("base_score", details.get("walmart_score", 0))
        
        # Performance adjustments
        if stats["requests"] > 0:
            success_rate = stats["successes"] / stats["requests"]
            score += success_rate * 10
            
            # Penalty for bot detections
            if stats["bot_detections"] > 0:
                detection_rate = stats["bot_detections"] / stats["requests"]
                score -= detection_rate * 20
        
        # Recency bonus
        if stats["last_success"]:
            hours_since_success = (datetime.now() - stats["last_success"]).total_seconds() / 3600
            if hours_since_success < 1:
                score += 5
            elif hours_since_success < 6:
                score += 2
        
        # Location bonus (US proxies preferred for Walmart)
        location = details.get("location", {})
        if location.get("countryCode") == "US":
            score += 3
        
        # Latency penalty
        if details.get("latency_ms"):
            if details["latency_ms"] > 2000:
                score -= 3
            elif details["latency_ms"] < 500:
                score += 2
        
        return max(0, score)
    
    def get_proxy(self, request_context: Optional[Dict] = None) -> Optional[str]:
        """
        Get best available proxy considering:
        - Current performance
        - Cooldown periods
        - Subnet diversity
        - Request context (URL type, retry count, etc.)
        """
        with self.lock:
            current_time = datetime.now()
            available_proxies = []
            
            # Reset subnet usage periodically
            if (current_time - self.last_subnet_reset).total_seconds() > 3600:
                self.subnet_usage.clear()
                self.last_subnet_reset = current_time
            
            for proxy in self.proxies:
                stats = self.proxy_stats[proxy]
                
                # Skip if in cooldown
                if stats["cooldown_until"] and current_time < stats["cooldown_until"]:
                    continue
                
                # Skip if used too recently (basic rate limiting)
                if stats["last_used"]:
                    seconds_since_use = (current_time - stats["last_used"]).total_seconds()
                    # Adaptive rate limiting based on success
                    min_interval = 2 if stats["successes"] > stats["failures"] else 10
                    if seconds_since_use < min_interval:
                        continue
                
                # Calculate subnet usage penalty
                subnet = self._get_subnet(proxy)
                subnet_penalty = self.subnet_usage[subnet] * 2
                
                # Calculate final score
                score = self._calculate_dynamic_score(proxy) - subnet_penalty
                
                # Context-based adjustments
                if request_context:
                    # Prefer US proxies for product pages
                    if "product" in request_context.get("url_type", ""):
                        if self.proxy_details.get(proxy, {}).get("location", {}).get("countryCode") == "US":
                            score += 5
                    
                    # Use different proxies for retries
                    if request_context.get("retry_count", 0) > 0:
                        if proxy == request_context.get("last_proxy"):
                            continue
                
                available_proxies.append((proxy, score))
            
            if not available_proxies:
                # Emergency reset - clear all cooldowns
                for proxy in self.proxies:
                    self.proxy_stats[proxy]["cooldown_until"] = None
                return random.choice(self.proxies) if self.proxies else None
            
            # Sort by score and select
            available_proxies.sort(key=lambda x: x[1], reverse=True)
            
            # Sometimes choose randomly from top performers to avoid patterns
            if random.random() < 0.2 and len(available_proxies) > 5:
                selected = random.choice(available_proxies[:5])[0]
            else:
                selected = available_proxies[0][0]
            
            # Update usage tracking
            self.proxy_stats[selected]["last_used"] = current_time
            self.proxy_stats[selected]["requests"] += 1
            self.subnet_usage[self._get_subnet(selected)] += 1
            
            return selected
    
    def record_success(self, proxy: str, response_time: Optional[float] = None,
                      user_agent: Optional[str] = None):
        """Record successful request with detailed context"""
        with self.lock:
            stats = self.proxy_stats[proxy]
            current_time = datetime.now()
            
            stats["successes"] += 1
            stats["last_success"] = current_time
            stats["cooldown_until"] = None  # Clear any cooldown
            
            # Update average response time
            if response_time:
                if stats["avg_response_time"] == 0:
                    stats["avg_response_time"] = response_time
                else:
                    stats["avg_response_time"] = (stats["avg_response_time"] + response_time) / 2
            
            # Track success patterns
            if user_agent:
                self.global_patterns["successful_user_agents"][user_agent] += 1
            
            self.global_patterns["successful_times"][current_time.hour] += 1
            
            # Track request intervals
            if stats["last_used"] and stats["last_success"]:
                interval = (current_time - stats["last_used"]).total_seconds()
                self.global_patterns["successful_request_intervals"].append(interval)
                # Keep only recent intervals
                if len(self.global_patterns["successful_request_intervals"]) > 1000:
                    self.global_patterns["successful_request_intervals"] = \
                        self.global_patterns["successful_request_intervals"][-1000:]
    
    def record_failure(self, proxy: str, error_type: str = "generic",
                      bot_detected: bool = False):
        """Record failed request with adaptive cooldown"""
        with self.lock:
            stats = self.proxy_stats[proxy]
            current_time = datetime.now()
            
            stats["failures"] += 1
            stats["last_failure"] = current_time
            
            if bot_detected:
                stats["bot_detections"] += 1
            
            # Adaptive cooldown based on failure type and history
            failure_rate = stats["failures"] / max(stats["requests"], 1)
            
            if bot_detected:
                # Severe cooldown for bot detection
                cooldown_minutes = min(60, 10 * (stats["bot_detections"] ** 2))
            elif failure_rate > 0.8:
                # High failure rate - long cooldown
                cooldown_minutes = 30
            elif failure_rate > 0.5:
                # Medium failure rate
                cooldown_minutes = 10
            else:
                # Low failure rate - short cooldown
                cooldown_minutes = 2
            
            stats["cooldown_until"] = current_time + timedelta(minutes=cooldown_minutes)
    
    def get_session_data(self, proxy: str) -> Dict:
        """Get persistent session data for a proxy"""
        return self.proxy_stats[proxy]["session_data"]
    
    def update_session_data(self, proxy: str, data: Dict):
        """Update session data for a proxy"""
        with self.lock:
            self.proxy_stats[proxy]["session_data"].update(data)
    
    def get_best_user_agent(self) -> str:
        """Get user agent with best success rate"""
        if self.global_patterns["successful_user_agents"]:
            # Return most successful user agent
            return max(self.global_patterns["successful_user_agents"].items(),
                      key=lambda x: x[1])[0]
        return None
    
    def get_optimal_request_interval(self) -> float:
        """Calculate optimal request interval based on success patterns"""
        intervals = self.global_patterns["successful_request_intervals"]
        if len(intervals) > 10:
            # Use median of successful intervals
            sorted_intervals = sorted(intervals)
            return sorted_intervals[len(sorted_intervals) // 2]
        return random.uniform(2, 5)  # Default
    
    def get_stats_summary(self) -> Dict:
        """Get comprehensive statistics summary"""
        with self.lock:
            total_requests = sum(s["requests"] for s in self.proxy_stats.values())
            total_successes = sum(s["successes"] for s in self.proxy_stats.values())
            total_failures = sum(s["failures"] for s in self.proxy_stats.values())
            total_detections = sum(s["bot_detections"] for s in self.proxy_stats.values())
            
            working_proxies = [p for p, s in self.proxy_stats.items() 
                             if s["successes"] > 0 and s["last_success"]]
            
            return {
                "total_proxies": len(self.proxies),
                "proxies_used": len([p for p, s in self.proxy_stats.items() if s["requests"] > 0]),
                "working_proxies": len(working_proxies),
                "total_requests": total_requests,
                "total_successes": total_successes,
                "total_failures": total_failures,
                "total_bot_detections": total_detections,
                "success_rate": total_successes / max(total_requests, 1),
                "detection_rate": total_detections / max(total_requests, 1),
                "proxies_in_cooldown": len([p for p, s in self.proxy_stats.items() 
                                           if s["cooldown_until"] and s["cooldown_until"] > datetime.now()]),
                "best_performing_proxies": self._get_top_proxies(5),
                "optimal_request_interval": self.get_optimal_request_interval(),
                "peak_hours": self._get_peak_hours()
            }
    
    def _get_top_proxies(self, n: int = 5) -> List[Dict]:
        """Get top performing proxies"""
        proxy_scores = []
        for proxy in self.proxies:
            if self.proxy_stats[proxy]["requests"] > 0:
                score = self._calculate_dynamic_score(proxy)
                proxy_scores.append({
                    "proxy": proxy,
                    "score": score,
                    "requests": self.proxy_stats[proxy]["requests"],
                    "success_rate": self.proxy_stats[proxy]["successes"] / self.proxy_stats[proxy]["requests"],
                    "location": self.proxy_details.get(proxy, {}).get("location", {}).get("countryCode", "Unknown")
                })
        
        proxy_scores.sort(key=lambda x: x["score"], reverse=True)
        return proxy_scores[:n]
    
    def _get_peak_hours(self) -> List[int]:
        """Get hours with highest success rates"""
        if not self.global_patterns["successful_times"]:
            return []
        
        hours = sorted(self.global_patterns["successful_times"].items(),
                      key=lambda x: x[1], reverse=True)
        return [h[0] for h in hours[:3]]
    
    def save_stats(self, filepath: str = "helpers/proxy_stats.pkl"):
        """Save statistics for persistence across runs"""
        with self.lock:
            data = {
                "proxy_stats": dict(self.proxy_stats),
                "global_patterns": dict(self.global_patterns),
                "subnet_usage": dict(self.subnet_usage)
            }
            
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "wb") as f:
                pickle.dump(data, f)
    
    def _load_stats(self, filepath: str = "helpers/proxy_stats.pkl"):
        """Load saved statistics"""
        try:
            with open(filepath, "rb") as f:
                data = pickle.load(f)
                
            # Convert back to defaultdicts
            for proxy, stats in data["proxy_stats"].items():
                if proxy in self.proxies:  # Only load stats for current proxies
                    self.proxy_stats[proxy].update(stats)
            
            self.global_patterns.update(data["global_patterns"])
            self.subnet_usage.update(data["subnet_usage"])
            
            print(f"Loaded historical stats for {len(self.proxy_stats)} proxies")
        except:
            print("No historical proxy stats found, starting fresh") 