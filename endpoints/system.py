"""
System status endpoints router.
Provides API endpoints for system status, service alerts, and statistics.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from database_connector import DatabaseConnector, get_db
from pydantic_models import SystemStatus, ServiceAlert
from endpoint_handlers.system_handlers import (
    get_system_status,
    get_active_alerts,
    get_system_stats,
    get_alerts_for_route,
    get_alerts_for_stop,
    get_alerts_by_type,
    get_alerts_by_time_range
)
from utils.caching import get_cache_headers
from utils.cache_management import get_cache_manager, get_cache_health, cleanup_expired_cache

router = APIRouter(prefix="/system", tags=["system"])


@router.get("/status", response_model=SystemStatus)
async def system_status(response: Response, db: DatabaseConnector = Depends(get_db)):
    """
    Get overall system status information.
    
    Returns current system health, operational routes count, and active alerts count.
    """
    try:
        # Add cache headers for client-side caching
        cache_headers = get_cache_headers(60)  # 1 minute
        for key, value in cache_headers.items():
            response.headers[key] = value
        
        status = get_system_status(db)
        return status
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "SYSTEM_ERROR",
                    "message": "Unable to retrieve system status",
                    "details": {"error": str(e)}
                }
            }
        )


@router.get("/alerts", response_model=List[ServiceAlert])
async def system_alerts(
    response: Response,
    severity: Optional[str] = Query(None, description="Filter by severity level (info, warning, severe)"),
    db: DatabaseConnector = Depends(get_db)
):
    """
    Get current active service alerts.
    
    Args:
        severity: Optional filter by severity level (info, warning, severe)
    
    Returns list of active service alerts.
    """
    try:
        # Add cache headers for client-side caching
        cache_headers = get_cache_headers(120)  # 2 minutes
        for key, value in cache_headers.items():
            response.headers[key] = value
        
        # Validate severity parameter
        if severity and severity not in ['info', 'warning', 'severe']:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": "Invalid severity level",
                        "details": {
                            "field": "severity",
                            "value": severity,
                            "constraint": "must be one of: info, warning, severe"
                        }
                    }
                }
            )
        
        alerts = get_active_alerts(db, severity)
        return alerts
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "SYSTEM_ERROR",
                    "message": "Unable to retrieve service alerts",
                    "details": {"error": str(e)}
                }
            }
        )


@router.get("/stats", response_model=Dict[str, Any])
async def system_stats(response: Response, db: DatabaseConnector = Depends(get_db)):
    """
    Get system usage and coverage statistics.
    
    Returns comprehensive statistics about routes, stops, trips, and coverage area.
    """
    try:
        # Add cache headers for client-side caching
        cache_headers = get_cache_headers(300)  # 5 minutes
        for key, value in cache_headers.items():
            response.headers[key] = value
        
        stats = get_system_stats(db)
        return stats
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "SYSTEM_ERROR",
                    "message": "Unable to retrieve system statistics",
                    "details": {"error": str(e)}
                }
            }
        )


@router.get("/alerts/route/{route_id}", response_model=List[ServiceAlert])
async def route_alerts(
    route_id: str,
    db: DatabaseConnector = Depends(get_db)
):
    """
    Get active alerts for a specific route.
    
    Args:
        route_id: Route identifier to get alerts for
    
    Returns list of service alerts affecting the specified route.
    """
    try:
        # Validate that route exists
        route_check_query = "SELECT COUNT(*) as count FROM routes WHERE route_id = ?"
        route_df = db.execute_df(route_check_query, [route_id])
        
        if route_df.iloc[0]['count'] == 0:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "code": "ROUTE_NOT_FOUND",
                        "message": f"Route with ID '{route_id}' not found",
                        "details": {"route_id": route_id}
                    }
                }
            )
        
        alerts = get_alerts_for_route(db, route_id)
        return alerts
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "SYSTEM_ERROR",
                    "message": "Unable to retrieve route alerts",
                    "details": {"error": str(e), "route_id": route_id}
                }
            }
        )


@router.get("/alerts/stop/{stop_id}", response_model=List[ServiceAlert])
async def stop_alerts(
    stop_id: str,
    db: DatabaseConnector = Depends(get_db)
):
    """
    Get active alerts for a specific stop.
    
    Args:
        stop_id: Stop identifier to get alerts for
    
    Returns list of service alerts affecting the specified stop or routes serving it.
    """
    try:
        # Validate that stop exists
        stop_check_query = "SELECT COUNT(*) as count FROM stops WHERE stop_id = ?"
        stop_df = db.execute_df(stop_check_query, [stop_id])
        
        if stop_df.iloc[0]['count'] == 0:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "code": "STOP_NOT_FOUND",
                        "message": f"Stop with ID '{stop_id}' not found",
                        "details": {"stop_id": stop_id}
                    }
                }
            )
        
        alerts = get_alerts_for_stop(db, stop_id)
        return alerts
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "SYSTEM_ERROR",
                    "message": "Unable to retrieve stop alerts",
                    "details": {"error": str(e), "stop_id": stop_id}
                }
            }
        )


@router.get("/alerts/type/{alert_type}", response_model=List[ServiceAlert])
async def alerts_by_type(
    alert_type: str,
    db: DatabaseConnector = Depends(get_db)
):
    """
    Get active alerts categorized by type.
    
    Args:
        alert_type: Type of alert (delay, suspension, detour, maintenance, emergency)
    
    Returns list of service alerts of the specified type.
    """
    try:
        # Validate alert type
        valid_types = ['delay', 'suspension', 'detour', 'maintenance', 'emergency']
        if alert_type not in valid_types:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": "Invalid alert type",
                        "details": {
                            "field": "alert_type",
                            "value": alert_type,
                            "constraint": f"must be one of: {', '.join(valid_types)}"
                        }
                    }
                }
            )
        
        alerts = get_alerts_by_type(db, alert_type)
        return alerts
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "SYSTEM_ERROR",
                    "message": "Unable to retrieve alerts by type",
                    "details": {"error": str(e), "alert_type": alert_type}
                }
            }
        )


@router.get("/alerts/timerange", response_model=List[ServiceAlert])
async def alerts_by_time_range(
    start_time: str = Query(..., description="Start time in ISO format (YYYY-MM-DDTHH:MM:SS)"),
    end_time: str = Query(..., description="End time in ISO format (YYYY-MM-DDTHH:MM:SS)"),
    db: DatabaseConnector = Depends(get_db)
):
    """
    Get alerts active within a specific time range.
    
    Args:
        start_time: Start time in ISO format
        end_time: End time in ISO format
    
    Returns list of service alerts active in the specified time range.
    """
    try:
        # Validate time format
        from datetime import datetime
        try:
            datetime.fromisoformat(start_time)
            datetime.fromisoformat(end_time)
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": "Invalid time format",
                        "details": {
                            "constraint": "Time must be in ISO format (YYYY-MM-DDTHH:MM:SS)",
                            "error": str(e)
                        }
                    }
                }
            )
        
        # Validate that start_time is before end_time
        start_dt = datetime.fromisoformat(start_time)
        end_dt = datetime.fromisoformat(end_time)
        
        if start_dt >= end_dt:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": "Invalid time range",
                        "details": {
                            "constraint": "start_time must be before end_time"
                        }
                    }
                }
            )
        
        alerts = get_alerts_by_time_range(db, start_time, end_time)
        return alerts
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "SYSTEM_ERROR",
                    "message": "Unable to retrieve alerts by time range",
                    "details": {"error": str(e)}
                }
            }
        )


@router.get("/cache/health", response_model=Dict[str, Any])
async def cache_health(response: Response):
    """
    Get cache health metrics and performance statistics.
    
    Returns cache hit rates, size information, and optimization recommendations.
    """
    try:
        # Add cache headers - short cache for health metrics
        cache_headers = get_cache_headers(30)  # 30 seconds
        for key, value in cache_headers.items():
            response.headers[key] = value
        
        health_info = get_cache_health()
        return health_info
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "SYSTEM_ERROR",
                    "message": "Unable to retrieve cache health information",
                    "details": {"error": str(e)}
                }
            }
        )


@router.post("/cache/cleanup")
async def cache_cleanup():
    """
    Clean up expired cache entries.
    
    Returns the number of entries that were removed from the cache.
    """
    try:
        removed_count = cleanup_expired_cache()
        return {
            "message": "Cache cleanup completed successfully",
            "removed_entries": removed_count,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "SYSTEM_ERROR",
                    "message": "Unable to perform cache cleanup",
                    "details": {"error": str(e)}
                }
            }
        )


@router.post("/cache/invalidate/{cache_type}")
async def invalidate_cache(
    cache_type: str,
    resource_id: Optional[str] = Query(None, description="Optional specific resource ID to invalidate")
):
    """
    Invalidate cache entries by type.
    
    Args:
        cache_type: Type of cache to invalidate (stops, routes, trips, system, all)
        resource_id: Optional specific resource ID to invalidate
    
    Returns confirmation of cache invalidation.
    """
    try:
        cache_manager = get_cache_manager()
        
        if cache_type == "stops":
            invalidated_count = cache_manager.invalidate_stop_cache(resource_id)
        elif cache_type == "routes":
            invalidated_count = cache_manager.invalidate_route_cache(resource_id)
        elif cache_type == "trips":
            invalidated_count = cache_manager.invalidate_trip_cache(resource_id)
        elif cache_type == "system":
            invalidated_count = cache_manager.invalidate_system_cache()
        elif cache_type == "all":
            cache_manager.invalidate_all_cache()
            invalidated_count = "all"
        else:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": "Invalid cache type",
                        "details": {
                            "field": "cache_type",
                            "value": cache_type,
                            "constraint": "must be one of: stops, routes, trips, system, all"
                        }
                    }
                }
            )
        
        return {
            "message": f"Cache invalidation completed for {cache_type}",
            "cache_type": cache_type,
            "resource_id": resource_id,
            "invalidated_entries": invalidated_count,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "SYSTEM_ERROR",
                    "message": "Unable to invalidate cache",
                    "details": {"error": str(e), "cache_type": cache_type}
                }
            }
        )