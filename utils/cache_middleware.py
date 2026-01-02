"""
Cache middleware for automatic cache management.
Provides periodic cache cleanup and performance monitoring.
"""

import asyncio
import time
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from utils.cache_management import get_cache_manager


class CacheMiddleware(BaseHTTPMiddleware):
    """
    Middleware for automatic cache management and performance monitoring.
    """
    
    def __init__(self, app, cleanup_interval: int = 300):  
        super().__init__(app)
        self.cleanup_interval = cleanup_interval
        self.last_cleanup = time.time()
        self.cache_manager = get_cache_manager()
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request and perform periodic cache maintenance.
        """
        
        current_time = time.time()
        if current_time - self.last_cleanup > self.cleanup_interval:
            
            asyncio.create_task(self._background_cleanup())
            self.last_cleanup = current_time
        
        
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        
        
        cache_stats = self.cache_manager.cache.get_stats()
        response.headers["X-Cache-Hit-Rate"] = str(round(cache_stats.get('hit_rate', 0), 3))
        response.headers["X-Cache-Size"] = str(cache_stats.get('cache_size', 0))
        response.headers["X-Process-Time"] = str(round(process_time, 3))
        
        return response
    
    async def _background_cleanup(self):
        """
        Perform background cache cleanup.
        """
        try:
            removed_count = self.cache_manager.cleanup_expired_entries()
            if removed_count > 0:
                print(f"Cache cleanup: removed {removed_count} expired entries")
        except Exception as e:
            print(f"Cache cleanup error: {e}")


def add_cache_middleware(app, cleanup_interval: int = 300):
    """
    Add cache middleware to FastAPI application.
    
    Args:
        app: FastAPI application instance
        cleanup_interval: Cleanup interval in seconds (default: 5 minutes)
    """
    app.add_middleware(CacheMiddleware, cleanup_interval=cleanup_interval)