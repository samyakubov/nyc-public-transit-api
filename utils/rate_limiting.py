"""
Rate limiting utilities for the transit API.
Implements request rate limiting, export size limits, and resource usage tracking.
"""

import time
from typing import Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
from collections import defaultdict, deque
import threading
from dataclasses import dataclass
from fastapi import Request


@dataclass
class RateLimit:
    """Rate limit configuration."""
    requests_per_minute: int
    requests_per_hour: int
    requests_per_day: int
    export_size_limit: int = 10000  # Maximum items per export
    request_size_limit: int = 1024 * 1024  # 1MB request size limit


@dataclass
class UsageRecord:
    """Usage tracking record."""
    timestamp: float
    request_size: int = 0
    export_size: int = 0


class RateLimiter:
    """Thread-safe rate limiter with multiple time windows."""
    
    def __init__(self):
        self._usage: Dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()
        
        # Default rate limits by endpoint category
        self._limits = {
            "default": RateLimit(
                requests_per_minute=60,
                requests_per_hour=1000,
                requests_per_day=10000,
                export_size_limit=1000,
                request_size_limit=1024 * 1024  # 1MB
            ),
            "search": RateLimit(
                requests_per_minute=30,
                requests_per_hour=500,
                requests_per_day=5000,
                export_size_limit=500,
                request_size_limit=512 * 1024  # 512KB
            ),
            "export": RateLimit(
                requests_per_minute=5,
                requests_per_hour=50,
                requests_per_day=200,
                export_size_limit=10000,
                request_size_limit=5 * 1024 * 1024  # 5MB
            ),
            "system": RateLimit(
                requests_per_minute=120,
                requests_per_hour=2000,
                requests_per_day=20000,
                export_size_limit=100,
                request_size_limit=256 * 1024  # 256KB
            )
        }
    
    def _get_client_id(self, request: Request) -> str:
        """Generate client identifier from request."""
        # Try to get API key first
        api_key = request.headers.get("X-API-Key")
        if api_key:
            return f"api_key:{api_key}"
        
        # Fall back to IP address
        client_ip = request.client.host if request.client else "unknown"
        
        # Consider X-Forwarded-For for proxy setups
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            client_ip = forwarded_for.split(",")[0].strip()
        
        return f"ip:{client_ip}"
    
    def _get_endpoint_category(self, path: str) -> str:
        """Determine rate limit category based on endpoint path."""
        if "/search" in path:
            return "search"
        elif "/export" in path or "format=" in path:
            return "export"
        elif "/system" in path:
            return "system"
        else:
            return "default"
    
    def _cleanup_old_records(self, records: deque, max_age_seconds: int):
        """Remove records older than max_age_seconds."""
        current_time = time.time()
        while records and current_time - records[0].timestamp > max_age_seconds:
            records.popleft()
    
    def _count_requests_in_window(self, records: deque, window_seconds: int) -> int:
        """Count requests within the specified time window."""
        current_time = time.time()
        count = 0
        for record in reversed(records):
            if current_time - record.timestamp <= window_seconds:
                count += 1
            else:
                break
        return count
    
    def _get_usage_stats(self, records: deque) -> Dict[str, int]:
        """Get usage statistics for different time windows."""
        return {
            "minute": self._count_requests_in_window(records, 60),
            "hour": self._count_requests_in_window(records, 3600),
            "day": self._count_requests_in_window(records, 86400)
        }
    
    def check_rate_limit(
        self,
        request: Request,
        export_size: Optional[int] = None,
        request_size: Optional[int] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if request is within rate limits.
        
        Args:
            request: FastAPI request object
            export_size: Number of items being exported (optional)
            request_size: Size of request in bytes (optional)
        
        Returns:
            Tuple of (is_allowed, rate_limit_info)
        """
        client_id = self._get_client_id(request)
        category = self._get_endpoint_category(request.url.path)
        limits = self._limits[category]
        
        with self._lock:
            records = self._usage[client_id]
            
            # Clean up old records (keep 25 hours of data)
            self._cleanup_old_records(records, 25 * 3600)
            
            # Get current usage stats
            usage = self._get_usage_stats(records)
            
            # Check rate limits
            rate_limit_info = {
                "category": category,
                "limits": {
                    "requests_per_minute": limits.requests_per_minute,
                    "requests_per_hour": limits.requests_per_hour,
                    "requests_per_day": limits.requests_per_day,
                    "export_size_limit": limits.export_size_limit,
                    "request_size_limit": limits.request_size_limit
                },
                "current_usage": usage,
                "remaining": {
                    "minute": max(0, limits.requests_per_minute - usage["minute"]),
                    "hour": max(0, limits.requests_per_hour - usage["hour"]),
                    "day": max(0, limits.requests_per_day - usage["day"])
                },
                "reset_times": {
                    "minute": (datetime.now() + timedelta(minutes=1)).isoformat(),
                    "hour": (datetime.now() + timedelta(hours=1)).isoformat(),
                    "day": (datetime.now() + timedelta(days=1)).isoformat()
                }
            }
            
            # Check request rate limits
            if usage["minute"] >= limits.requests_per_minute:
                rate_limit_info["exceeded"] = "requests_per_minute"
                rate_limit_info["retry_after"] = 60
                return False, rate_limit_info
            
            if usage["hour"] >= limits.requests_per_hour:
                rate_limit_info["exceeded"] = "requests_per_hour"
                rate_limit_info["retry_after"] = 3600
                return False, rate_limit_info
            
            if usage["day"] >= limits.requests_per_day:
                rate_limit_info["exceeded"] = "requests_per_day"
                rate_limit_info["retry_after"] = 86400
                return False, rate_limit_info
            
            # Check export size limit
            if export_size and export_size > limits.export_size_limit:
                rate_limit_info["exceeded"] = "export_size_limit"
                rate_limit_info["export_size"] = export_size
                return False, rate_limit_info
            
            # Check request size limit
            if request_size and request_size > limits.request_size_limit:
                rate_limit_info["exceeded"] = "request_size_limit"
                rate_limit_info["request_size"] = request_size
                return False, rate_limit_info
            
            # Record this request
            records.append(UsageRecord(
                timestamp=time.time(),
                request_size=request_size or 0,
                export_size=export_size or 0
            ))
            
            return True, rate_limit_info
    
    def get_rate_limit_headers(self, rate_limit_info: Dict[str, Any]) -> Dict[str, str]:
        """Generate rate limit headers for response."""
        headers = {}
        
        if "limits" in rate_limit_info and "remaining" in rate_limit_info:
            limits = rate_limit_info["limits"]
            remaining = rate_limit_info["remaining"]
            reset_times = rate_limit_info.get("reset_times", {})
            
            # Standard rate limit headers
            headers["X-RateLimit-Limit-Minute"] = str(limits["requests_per_minute"])
            headers["X-RateLimit-Limit-Hour"] = str(limits["requests_per_hour"])
            headers["X-RateLimit-Limit-Day"] = str(limits["requests_per_day"])
            
            headers["X-RateLimit-Remaining-Minute"] = str(remaining["minute"])
            headers["X-RateLimit-Remaining-Hour"] = str(remaining["hour"])
            headers["X-RateLimit-Remaining-Day"] = str(remaining["day"])
            
            if "minute" in reset_times:
                headers["X-RateLimit-Reset-Minute"] = reset_times["minute"]
            if "hour" in reset_times:
                headers["X-RateLimit-Reset-Hour"] = reset_times["hour"]
            if "day" in reset_times:
                headers["X-RateLimit-Reset-Day"] = reset_times["day"]
            
            # Export and request size limits
            headers["X-Export-Size-Limit"] = str(limits["export_size_limit"])
            headers["X-Request-Size-Limit"] = str(limits["request_size_limit"])
            
            # Category information
            headers["X-RateLimit-Category"] = rate_limit_info.get("category", "default")
        
        if "retry_after" in rate_limit_info:
            headers["Retry-After"] = str(rate_limit_info["retry_after"])
        
        return headers
    
    def update_limits(self, category: str, limits: RateLimit):
        """Update rate limits for a specific category."""
        with self._lock:
            self._limits[category] = limits
    
    def get_usage_summary(self) -> Dict[str, Any]:
        """Get overall usage summary for monitoring."""
        with self._lock:
            summary = {
                "total_clients": len(self._usage),
                "categories": list(self._limits.keys()),
                "active_clients": 0,
                "total_requests_last_hour": 0
            }
            
            current_time = time.time()
            for client_id, records in self._usage.items():
                # Count requests in last hour
                hour_requests = self._count_requests_in_window(records, 3600)
                if hour_requests > 0:
                    summary["active_clients"] += 1
                    summary["total_requests_last_hour"] += hour_requests
            
            return summary
    
    def reset_client_usage(self, client_id: str):
        """Reset usage for a specific client (admin function)."""
        with self._lock:
            if client_id in self._usage:
                self._usage[client_id].clear()
    
    def get_client_usage(self, client_id: str) -> Optional[Dict[str, Any]]:
        """Get usage statistics for a specific client."""
        with self._lock:
            if client_id not in self._usage:
                return None
            
            records = self._usage[client_id]
            usage = self._get_usage_stats(records)
            
            return {
                "client_id": client_id,
                "usage": usage,
                "total_requests": len(records),
                "first_request": datetime.fromtimestamp(records[0].timestamp).isoformat() if records else None,
                "last_request": datetime.fromtimestamp(records[-1].timestamp).isoformat() if records else None
            }


# Global rate limiter instance
rate_limiter = RateLimiter()


def get_request_size(request: Request) -> int:
    """Estimate request size in bytes."""
    size = 0
    
    # Headers size
    for name, value in request.headers.items():
        size += len(name.encode()) + len(value.encode()) + 4  # ": " and "\r\n"
    
    # URL size
    size += len(str(request.url).encode())
    
    # Method size
    size += len(request.method.encode())
    
    # Body size (if available)
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            size += int(content_length)
        except ValueError:
            pass
    
    return size


async def check_rate_limits(
    request: Request,
    export_size: Optional[int] = None
) -> Dict[str, Any]:
    """
    Middleware function to check rate limits.
    
    Args:
        request: FastAPI request object
        export_size: Number of items being exported (optional)
    
    Returns:
        Rate limit information dictionary
    
    Raises:
        HTTPException: If rate limit is exceeded
    """
    from utils.error_handling import error_handler
    
    request_size = get_request_size(request)
    
    is_allowed, rate_limit_info = rate_limiter.check_rate_limit(
        request=request,
        export_size=export_size,
        request_size=request_size
    )
    
    if not is_allowed:
        exceeded_limit = rate_limit_info.get("exceeded")
        
        if exceeded_limit == "export_size_limit":
            error_handler.handle_rate_limit_error(
                limit_type="export",
                current_usage=rate_limit_info.get("export_size", 0),
                limit=rate_limit_info["limits"]["export_size_limit"],
                request_id=request.headers.get("X-Request-ID")
            )
        elif exceeded_limit == "request_size_limit":
            error_handler.handle_rate_limit_error(
                limit_type="request_size",
                current_usage=rate_limit_info.get("request_size", 0),
                limit=rate_limit_info["limits"]["request_size_limit"],
                request_id=request.headers.get("X-Request-ID")
            )
        else:
            # Request rate limit exceeded
            current_usage = rate_limit_info["current_usage"]
            limits = rate_limit_info["limits"]
            
            if exceeded_limit == "requests_per_minute":
                limit_value = limits["requests_per_minute"]
                usage_value = current_usage["minute"]
            elif exceeded_limit == "requests_per_hour":
                limit_value = limits["requests_per_hour"]
                usage_value = current_usage["hour"]
            else:  # requests_per_day
                limit_value = limits["requests_per_day"]
                usage_value = current_usage["day"]
            
            error_handler.handle_rate_limit_error(
                limit_type=exceeded_limit,
                current_usage=usage_value,
                limit=limit_value,
                reset_time=rate_limit_info.get("reset_times", {}).get(exceeded_limit.split("_")[-1]),
                request_id=request.headers.get("X-Request-ID")
            )
    
    return rate_limit_info


class RateLimitMiddleware:
    """FastAPI middleware for automatic rate limiting."""
    
    def __init__(self, app, exclude_paths: Optional[list] = None):
        self.app = app
        self.exclude_paths = exclude_paths or ["/docs", "/redoc", "/openapi.json", "/health"]
    
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        request = Request(scope, receive)
        
        # Skip rate limiting for excluded paths
        if any(request.url.path.startswith(path) for path in self.exclude_paths):
            await self.app(scope, receive, send)
            return
        
        # Check rate limits
        try:
            rate_limit_info = await check_rate_limits(request)
            
            # Add rate limit headers to response
            async def send_with_headers(message):
                if message["type"] == "http.response.start":
                    headers = rate_limiter.get_rate_limit_headers(rate_limit_info)
                    for name, value in headers.items():
                        message["headers"].append([name.encode(), value.encode()])
                await send(message)
            
            await self.app(scope, receive, send_with_headers)
            
        except Exception as e:
            # Rate limit exceeded - error already raised by check_rate_limits
            await self.app(scope, receive, send)


def add_rate_limiting_middleware(app, exclude_paths: Optional[list] = None):
    """Add rate limiting middleware to FastAPI app."""
    app.add_middleware(RateLimitMiddleware, exclude_paths=exclude_paths)


# Export size validation decorator
def validate_export_size(max_size: Optional[int] = None):
    """Decorator to validate export size limits."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Extract request from args/kwargs
            request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            
            if not request:
                # Look in kwargs
                request = kwargs.get('request')
            
            if request:
                # Check if this is an export request
                export_size = kwargs.get('limit', kwargs.get('per_page', 0))
                if export_size and max_size and export_size > max_size:
                    from utils.error_handling import error_handler
                    error_handler.handle_rate_limit_error(
                        limit_type="export",
                        current_usage=export_size,
                        limit=max_size,
                        request_id=request.headers.get("X-Request-ID")
                    )
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator