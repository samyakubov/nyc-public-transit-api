"""
Cache management utilities for the transit API.
Provides cache invalidation strategies and cache warming functionality.
"""

from typing import List, Optional, Dict, Any
from endpoint_handlers.route_handlers.get_all_routes import get_all_routes
from utils.caching import get_global_cache, invalidate_cache_pattern
from database_connector import DatabaseConnector
from endpoint_handlers.system_handlers import get_system_status, get_system_stats

class CacheManager:
    """
    Manages cache invalidation strategies and cache warming for the transit API.
    """
    
    def __init__(self):
        self.cache = get_global_cache()
        self._invalidation_patterns = {
            'stops': ['get_stop_by_id_handler', 'search_stops_handler', 'get_stop_routes_handler'],
            'routes': ['get_all_routes', 'get_route_by_id', 'get_route_stops', 'get_route_shape'],
            'trips': ['get_route_trips', 'get_stop_departures_handler'],
            'system': ['get_system_status', 'get_active_alerts', 'get_system_stats'],
            'geospatial': ['get_nearby_stops_handler', 'get_nearby_routes']
        }
    
    def invalidate_stop_cache(self, stop_id: Optional[str] = None) -> int:
        """
        Invalidate cache entries related to stops.
        
        Args:
            stop_id: Optional specific stop ID to invalidate, or None for all stops
            
        Returns:
            Number of cache entries invalidated
        """
        invalidated_count = 0
        
        if stop_id:
            # Invalidate specific stop-related cache entries
            patterns = [
                f"get_stop_by_id_handler:{stop_id}",
                f"get_stop_routes_handler:{stop_id}",
                f"get_stop_departures_handler:{stop_id}"
            ]
            for pattern in patterns:
                invalidated_count += invalidate_cache_pattern(pattern)
        else:
            # Invalidate all stop-related cache entries
            for pattern in self._invalidation_patterns['stops']:
                invalidated_count += invalidate_cache_pattern(pattern)
            
            # Also invalidate geospatial queries that might include stops
            for pattern in self._invalidation_patterns['geospatial']:
                invalidated_count += invalidate_cache_pattern(pattern)
        
        return invalidated_count
    
    def invalidate_route_cache(self, route_id: Optional[str] = None) -> int:
        """
        Invalidate cache entries related to routes.
        
        Args:
            route_id: Optional specific route ID to invalidate, or None for all routes
            
        Returns:
            Number of cache entries invalidated
        """
        invalidated_count = 0
        
        if route_id:
            # Invalidate specific route-related cache entries
            patterns = [
                f"get_route_by_id:{route_id}",
                f"get_route_stops:{route_id}",
                f"get_route_trips:{route_id}",
                f"get_route_shape:{route_id}"
            ]
            for pattern in patterns:
                invalidated_count += invalidate_cache_pattern(pattern)
        else:
            # Invalidate all route-related cache entries
            for pattern in self._invalidation_patterns['routes']:
                invalidated_count += invalidate_cache_pattern(pattern)
            
            # Also invalidate geospatial queries that might include routes
            for pattern in self._invalidation_patterns['geospatial']:
                invalidated_count += invalidate_cache_pattern(pattern)
        
        return invalidated_count
    
    def invalidate_trip_cache(self, route_id: Optional[str] = None) -> int:
        """
        Invalidate cache entries related to trips and schedules.
        
        Args:
            route_id: Optional specific route ID to invalidate trips for
            
        Returns:
            Number of cache entries invalidated
        """
        invalidated_count = 0
        
        if route_id:
            # Invalidate trip-related cache entries for specific route
            patterns = [
                f"get_route_trips:{route_id}",
                f"get_stop_departures_handler"  # Departures might be affected
            ]
            for pattern in patterns:
                invalidated_count += invalidate_cache_pattern(pattern)
        else:
            # Invalidate all trip-related cache entries
            for pattern in self._invalidation_patterns['trips']:
                invalidated_count += invalidate_cache_pattern(pattern)
        
        return invalidated_count
    
    def invalidate_system_cache(self) -> int:
        """
        Invalidate cache entries related to system status and alerts.
        
        Returns:
            Number of cache entries invalidated
        """
        invalidated_count = 0
        
        for pattern in self._invalidation_patterns['system']:
            invalidated_count += invalidate_cache_pattern(pattern)
        
        return invalidated_count
    
    def invalidate_all_cache(self) -> None:
        """
        Clear all cache entries.
        """
        self.cache.clear()
    
    def get_cache_health(self) -> Dict[str, Any]:
        """
        Get cache health metrics and statistics.
        
        Returns:
            Dictionary with cache health information
        """
        stats = self.cache.get_stats()
        cache_info = self.cache.get_cache_info()
        
        # Calculate cache health score based on hit rate and size
        hit_rate = stats.get('hit_rate', 0)
        cache_size = stats.get('cache_size', 0)
        
        # Health score: 100% for hit rate > 0.8, scaled down for lower rates
        health_score = min(100, hit_rate * 125)  # 0.8 hit rate = 100 score
        
        # Adjust for cache size (too small might indicate issues)
        if cache_size < 10:
            health_score *= 0.8  # Reduce score if cache is too small
        
        health_status = "excellent" if health_score >= 80 else \
                       "good" if health_score >= 60 else \
                       "fair" if health_score >= 40 else "poor"
        
        return {
            "health_score": round(health_score, 2),
            "health_status": health_status,
            "statistics": stats,
            "recommendations": self._get_cache_recommendations(stats, cache_info)
        }
    
    def _get_cache_recommendations(self, stats: Dict[str, Any], cache_info: Dict[str, Any]) -> List[str]:
        """
        Generate cache optimization recommendations based on current metrics.
        
        Args:
            stats: Cache statistics
            cache_info: Detailed cache information
            
        Returns:
            List of recommendation strings
        """
        recommendations = []
        
        hit_rate = stats.get('hit_rate', 0)
        cache_size = stats.get('cache_size', 0)
        total_requests = stats.get('total_requests', 0)
        
        if hit_rate < 0.5:
            recommendations.append("Low cache hit rate detected. Consider increasing TTL values for stable data.")
        
        if cache_size < 10 and total_requests > 100:
            recommendations.append("Cache size is small relative to request volume. Check TTL settings.")
        
        if cache_size > 1000:
            recommendations.append("Large cache size detected. Consider implementing cache size limits.")
        
        # Check for expired entries
        expired_count = sum(1 for entry in cache_info.get('entries', []) if entry.get('is_expired', False))
        if expired_count > cache_size * 0.2:
            recommendations.append("High number of expired entries. Consider running cache cleanup.")
        
        # Check for low-access entries
        low_access_count = sum(1 for entry in cache_info.get('entries', []) if entry.get('access_count', 0) <= 1)
        if low_access_count > cache_size * 0.3:
            recommendations.append("Many cache entries have low access counts. Consider adjusting caching strategy.")
        
        if not recommendations:
            recommendations.append("Cache is performing well. No immediate optimizations needed.")
        
        return recommendations
    
    def cleanup_expired_entries(self) -> int:
        """
        Remove expired entries from the cache.
        
        Returns:
            Number of entries removed
        """
        return self.cache.cleanup_expired()
    
    async def warm_cache(self, db: DatabaseConnector) -> Dict[str, int]:
        """
        Pre-populate cache with frequently accessed data.
        
        Args:
            db: Database connector instance
            
        Returns:
            Dictionary with counts of warmed cache entries by category
        """
        warmed_counts = {
            'routes': 0,
            'stops': 0,
            'system': 0
        }
        
        try:

            
            # Warm route cache with basic route list
            routes = get_all_routes(db, limit=50)  # Cache top 50 routes
            warmed_counts['routes'] = len(routes)
            
            # Warm system cache
            get_system_status(db)
            get_system_stats(db)
            warmed_counts['system'] = 2
            
            # Note: We don't warm stop cache as it's location-dependent
            # and would require specific coordinates
            
        except Exception as e:
            # Cache warming is optional, don't fail if it doesn't work
            pass
        
        return warmed_counts


# Global cache manager instance
_cache_manager = CacheManager()


def get_cache_manager() -> CacheManager:
    """Get the global cache manager instance."""
    return _cache_manager


def invalidate_stop_cache(stop_id: Optional[str] = None) -> int:
    """Convenience function to invalidate stop cache."""
    return _cache_manager.invalidate_stop_cache(stop_id)


def invalidate_route_cache(route_id: Optional[str] = None) -> int:
    """Convenience function to invalidate route cache."""
    return _cache_manager.invalidate_route_cache(route_id)


def invalidate_trip_cache(route_id: Optional[str] = None) -> int:
    """Convenience function to invalidate trip cache."""
    return _cache_manager.invalidate_trip_cache(route_id)


def invalidate_system_cache() -> int:
    """Convenience function to invalidate system cache."""
    return _cache_manager.invalidate_system_cache()


def get_cache_health() -> Dict[str, Any]:
    """Convenience function to get cache health metrics."""
    return _cache_manager.get_cache_health()


def cleanup_expired_cache() -> int:
    """Convenience function to cleanup expired cache entries."""
    return _cache_manager.cleanup_expired_entries()