"""
Caching utilities for the transit API.
Provides in-memory caching with TTL support and cache invalidation strategies.
"""

import time
from typing import Any, Optional, Dict, Callable, Union
from functools import wraps
from datetime import datetime, timedelta
import hashlib
import json


class CacheEntry:
    """Represents a single cache entry with TTL support."""
    
    def __init__(self, value: Any, ttl_seconds: Optional[int] = None):
        self.value = value
        self.created_at = time.time()
        self.ttl_seconds = ttl_seconds
        self.access_count = 0
        self.last_accessed = self.created_at
    
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        if self.ttl_seconds is None:
            return False
        return time.time() - self.created_at > self.ttl_seconds
    
    def access(self) -> Any:
        """Access the cached value and update access statistics."""
        self.access_count += 1
        self.last_accessed = time.time()
        return self.value


class InMemoryCache:
    """Simple in-memory cache with TTL support."""
    
    def __init__(self, default_ttl: Optional[int] = 300):  
        self._cache: Dict[str, CacheEntry] = {}
        self.default_ttl = default_ttl
        self._stats = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'deletes': 0,
            'evictions': 0
        }
    
    def _generate_key(self, key: Union[str, tuple, dict]) -> str:
        """Generate a string key from various input types."""
        if isinstance(key, str):
            return key
        elif isinstance(key, (tuple, list)):
            return hashlib.md5(str(sorted(key)).encode()).hexdigest()
        elif isinstance(key, dict):
            return hashlib.md5(json.dumps(key, sort_keys=True).encode()).hexdigest()
        else:
            return hashlib.md5(str(key).encode()).hexdigest()
    
    def get(self, key: Union[str, tuple, dict]) -> Optional[Any]:
        """Get a value from the cache."""
        cache_key = self._generate_key(key)
        
        if cache_key in self._cache:
            entry = self._cache[cache_key]
            
            if entry.is_expired():
                del self._cache[cache_key]
                self._stats['evictions'] += 1
                self._stats['misses'] += 1
                return None
            
            self._stats['hits'] += 1
            return entry.access()
        
        self._stats['misses'] += 1
        return None
    
    def set(self, key: Union[str, tuple, dict], value: Any, ttl: Optional[int] = None) -> None:
        """Set a value in the cache."""
        cache_key = self._generate_key(key)
        ttl_to_use = ttl if ttl is not None else self.default_ttl
        
        self._cache[cache_key] = CacheEntry(value, ttl_to_use)
        self._stats['sets'] += 1
    
    def delete(self, key: Union[str, tuple, dict]) -> bool:
        """Delete a value from the cache."""
        cache_key = self._generate_key(key)
        
        if cache_key in self._cache:
            del self._cache[cache_key]
            self._stats['deletes'] += 1
            return True
        
        return False
    
    def clear(self) -> None:
        """Clear all entries from the cache."""
        self._cache.clear()
        self._stats = {k: 0 for k in self._stats}
    
    def cleanup_expired(self) -> int:
        """Remove expired entries and return count of removed entries."""
        expired_keys = []
        
        for key, entry in self._cache.items():
            if entry.is_expired():
                expired_keys.append(key)
        
        for key in expired_keys:
            del self._cache[key]
            self._stats['evictions'] += 1
        
        return len(expired_keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self._stats['hits'] + self._stats['misses']
        hit_rate = self._stats['hits'] / total_requests if total_requests > 0 else 0
        
        return {
            **self._stats,
            'total_requests': total_requests,
            'hit_rate': hit_rate,
            'cache_size': len(self._cache),
            'memory_usage_estimate': sum(
                len(str(entry.value)) for entry in self._cache.values()
            )
        }
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get detailed cache information."""
        now = time.time()
        entries_info = []
        
        for key, entry in self._cache.items():
            entries_info.append({
                'key': key,
                'created_at': datetime.fromtimestamp(entry.created_at).isoformat(),
                'last_accessed': datetime.fromtimestamp(entry.last_accessed).isoformat(),
                'access_count': entry.access_count,
                'ttl_seconds': entry.ttl_seconds,
                'expires_at': datetime.fromtimestamp(
                    entry.created_at + entry.ttl_seconds
                ).isoformat() if entry.ttl_seconds else None,
                'is_expired': entry.is_expired(),
                'size_estimate': len(str(entry.value))
            })
        
        return {
            'stats': self.get_stats(),
            'entries': entries_info
        }


_global_cache = InMemoryCache()


def cached(ttl: Optional[int] = None, key_func: Optional[Callable] = None):
    """
    Decorator for caching function results.
    
    Args:
        ttl: Time to live in seconds
        key_func: Function to generate cache key from function arguments
    
    Returns:
        Decorated function with caching
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = f"{func.__name__}:{hash((args, tuple(sorted(kwargs.items()))))}"
            
            
            cached_result = _global_cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            
            result = func(*args, **kwargs)
            _global_cache.set(cache_key, result, ttl)
            
            return result
        
        
        wrapper.cache_clear = lambda: _global_cache.clear()
        wrapper.cache_info = lambda: _global_cache.get_stats()
        
        return wrapper
    
    return decorator


def get_cache_headers(ttl_seconds: Optional[int] = None) -> Dict[str, str]:
    """
    Generate HTTP cache headers for API responses.
    
    Args:
        ttl_seconds: Cache TTL in seconds
    
    Returns:
        Dictionary of HTTP headers
    """
    headers = {}
    
    if ttl_seconds:
        headers['Cache-Control'] = f'public, max-age={ttl_seconds}'
        expires_at = datetime.utcnow() + timedelta(seconds=ttl_seconds)
        headers['Expires'] = expires_at.strftime('%a, %d %b %Y %H:%M:%S GMT')
    else:
        headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        headers['Pragma'] = 'no-cache'
        headers['Expires'] = '0'
    
    return headers


def invalidate_cache_pattern(pattern: str) -> int:
    """
    Invalidate cache entries matching a pattern.
    
    Args:
        pattern: Pattern to match against cache keys
    
    Returns:
        Number of entries invalidated
    """
    keys_to_delete = []
    
    for key in _global_cache._cache.keys():
        if pattern in key:
            keys_to_delete.append(key)
    
    for key in keys_to_delete:
        _global_cache.delete(key)
    
    return len(keys_to_delete)


def get_global_cache() -> InMemoryCache:
    """Get the global cache instance."""
    return _global_cache


def clear_global_cache() -> None:
    """Clear the global cache."""
    _global_cache.clear()


def get_cache_stats() -> Dict[str, Any]:
    """Get global cache statistics."""
    return _global_cache.get_stats()