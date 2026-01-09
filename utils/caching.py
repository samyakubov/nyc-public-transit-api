import time
import threading
from typing import Any, Optional, Dict, Callable, Union
from functools import wraps
from datetime import datetime, timedelta
import hashlib
import json


class CacheEntry:
    def __init__(self, value: Any, ttl_seconds: Optional[int] = None):
        self.value = value
        self.created_at = time.time()
        self.ttl_seconds = ttl_seconds
        self.access_count = 0
        self.last_accessed = self.created_at

    def is_expired(self) -> bool:
        if self.ttl_seconds is None:
            return False
        return time.time() - self.created_at > self.ttl_seconds

    def access(self) -> Any:
        self.access_count += 1
        self.last_accessed = time.time()
        return self.value


class InMemoryCache:
    def __init__(self, default_ttl: Optional[int] = 300):
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self.default_ttl = default_ttl
        self._stats = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'deletes': 0,
            'evictions': 0
        }

    def _generate_key(self, key: Union[str, tuple, dict]) -> str:
        if isinstance(key, str):
            return key
        if isinstance(key, (tuple, list)):
            return hashlib.md5(str(sorted(key)).encode()).hexdigest()
        if isinstance(key, dict):
            return hashlib.md5(json.dumps(key, sort_keys=True).encode()).hexdigest()
        return hashlib.md5(str(key).encode()).hexdigest()

    def get(self, key):
        cache_key = self._generate_key(key)
        with self._lock:
            entry = self._cache.get(cache_key)
            if not entry:
                self._stats['misses'] += 1
                return None

            if entry.is_expired():
                del self._cache[cache_key]
                self._stats['evictions'] += 1
                self._stats['misses'] += 1
                return None

            self._stats['hits'] += 1
            return entry.access()

    def set(self, key, value, ttl: Optional[int] = None):
        cache_key = self._generate_key(key)
        ttl_to_use = ttl if ttl is not None else self.default_ttl
        with self._lock:
            self._cache[cache_key] = CacheEntry(value, ttl_to_use)
            self._stats['sets'] += 1

    def delete(self, key) -> bool:
        cache_key = self._generate_key(key)
        with self._lock:
            if cache_key in self._cache:
                del self._cache[cache_key]
                self._stats['deletes'] += 1
                return True
            return False

    def clear(self):
        with self._lock:
            self._cache.clear()
            self._stats = {k: 0 for k in self._stats}

    def cleanup_expired(self) -> int:
        with self._lock:
            expired = [k for k, v in self._cache.items() if v.is_expired()]
            for k in expired:
                del self._cache[k]
                self._stats['evictions'] += 1
            return len(expired)

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            values = list(self._cache.values())
            total_requests = self._stats['hits'] + self._stats['misses']
            hit_rate = self._stats['hits'] / total_requests if total_requests else 0

            return {
                **self._stats,
                'total_requests': total_requests,
                'hit_rate': hit_rate,
                'cache_size': len(values),
                'memory_usage_estimate': sum(len(str(e.value)) for e in values)
            }

    def get_cache_info(self) -> Dict[str, Any]:
        with self._lock:
            items = list(self._cache.items())

        entries_info = []
        for key, entry in items:
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
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = (
                key_func(*args, **kwargs)
                if key_func
                else f"{func.__name__}:{hash((args, tuple(sorted(kwargs.items()))))}"
            )

            cached_result = _global_cache.get(cache_key)
            if cached_result is not None:
                return cached_result

            result = func(*args, **kwargs)
            _global_cache.set(cache_key, result, ttl)
            return result

        wrapper.cache_clear = _global_cache.clear
        wrapper.cache_info = _global_cache.get_stats
        return wrapper

    return decorator
