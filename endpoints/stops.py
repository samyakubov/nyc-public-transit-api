from fastapi import APIRouter, Query, Depends, HTTPException, Path, Response, Request
from typing import Optional, List
from database_connector import get_db, DatabaseConnector
from endpoint_handlers.stop_handlers.get_nearby_stops import get_nearby_stops_handler

from endpoint_handlers.stop_handlers.get_stop_by_id import get_stop_by_id_handler

from endpoint_handlers.stop_handlers.fuzzy_search_stops import search_stops_handler

from endpoint_handlers.stop_handlers.get_stop_routes import get_stop_routes_handler

from endpoint_handlers.stop_handlers.get_stop_departures import get_stop_departures_handler
from endpoint_handlers.trip_handlers.get_trip_by_time import get_stop_departures_by_time

from pydantic_models import (
    Stop, StopDeparture,
    GeoJSONResponse, RouteBasic
)
from utils.caching import get_cache_headers
from utils.rate_limiting import check_rate_limits, rate_limiter
from utils.resource_limits import ResourceLimitValidator


stop_routes = APIRouter(prefix="/stops")

@stop_routes.get("/nearby", response_model=GeoJSONResponse)
@ResourceLimitValidator.validate_export_limits(max_size=100)
async def get_nearby_stops(
    request: Request,
    response: Response,
    lat: float = Query(..., description="Latitude", ge=-90, le=90),
    lon: float = Query(..., description="Longitude", ge=-180, le=180),
    radius_miles: float = Query(0.5, description="Search radius in miles", gt=0, le=10),
    limit: int = Query(50, description="Maximum number of stops to return", ge=1, le=100),
    db: DatabaseConnector = Depends(get_db)
):
    """
    Find stops within a specified radius of a point.
    
    Returns stops as GeoJSON features with distance information.
    Resource limits: Maximum 100 stops per request.
    """
    # Rate limiting is handled by middleware, but we can add custom headers
    rate_limit_info = await check_rate_limits(request)
    rate_limit_headers = rate_limiter.get_rate_limit_headers(rate_limit_info)
    
    # Add cache headers for client-side caching
    cache_headers = get_cache_headers(300)  # 5 minutes
    
    # Combine all headers
    all_headers = {**cache_headers, **rate_limit_headers}
    for key, value in all_headers.items():
        response.headers[key] = value
    
    return get_nearby_stops_handler(db, lat, lon, radius_miles, limit)

@stop_routes.get("/{stop_id}", response_model=Stop)
def get_stop_by_id(
    response: Response,
    stop_id: str = Path(..., description="Unique identifier for the stop"),
    db: DatabaseConnector = Depends(get_db)
):
    """
    Get detailed information for a specific stop by ID.
    
    Returns complete stop information including location and accessibility features.
    """
    # Add cache headers for client-side caching
    cache_headers = get_cache_headers(600)  # 10 minutes
    for key, value in cache_headers.items():
        response.headers[key] = value
    
    return get_stop_by_id_handler(db, stop_id)

@stop_routes.get("/search", response_model=List[Stop])
@ResourceLimitValidator.validate_export_limits(max_size=500)
async def search_stops(
    request: Request,
    response: Response,
    q: str = Query(..., description="Search query for stop names", min_length=1),
    limit: int = Query(20, description="Maximum number of results", ge=1, le=500),
    db: DatabaseConnector = Depends(get_db)
):
    """
    Search for stops by name using fuzzy matching.
    
    Returns stops that match the search query, ordered by relevance.
    Resource limits: Maximum 500 stops per search request.
    """
    # Check rate limits and get headers
    rate_limit_info = await check_rate_limits(request)
    rate_limit_headers = rate_limiter.get_rate_limit_headers(rate_limit_info)
    
    # Add cache headers for client-side caching
    cache_headers = get_cache_headers(300)  # 5 minutes
    
    # Combine all headers
    all_headers = {**cache_headers, **rate_limit_headers}
    for key, value in all_headers.items():
        response.headers[key] = value
    
    return search_stops_handler(db, q, limit)

@stop_routes.get("/{stop_id}/routes", response_model=List[RouteBasic])
def get_stop_routes(
    response: Response,
    stop_id: str = Path(..., description="Unique identifier for the stop"),
    db: DatabaseConnector = Depends(get_db)
):
    """
    Get all routes that serve a specific stop.
    
    Returns basic route information for all routes stopping at this location.
    """
    # Add cache headers for client-side caching
    cache_headers = get_cache_headers(600)  # 10 minutes
    for key, value in cache_headers.items():
        response.headers[key] = value
    
    return get_stop_routes_handler(db, stop_id)

@stop_routes.get("/{stop_id}/departures", response_model=List[StopDeparture])
@ResourceLimitValidator.validate_time_windows()
async def get_stop_departures(
    request: Request,
    response: Response,
    stop_id: str = Path(..., description="Unique identifier for the stop"),
    limit: int = Query(10, description="Maximum number of departures", ge=1, le=50),
    time_window_hours: int = Query(2, description="Time window in hours from now", ge=1, le=24),
    start_time: Optional[str] = Query(None, description="Start time in HH:MM:SS format (overrides current time)", regex=r'^\d{1,2}:\d{2}:\d{2}$'),
    end_time: Optional[str] = Query(None, description="End time in HH:MM:SS format (overrides time_window_hours)", regex=r'^\d{1,2}:\d{2}:\d{2}$'),
    db: DatabaseConnector = Depends(get_db)
):
    """
    Get upcoming departures from a specific stop.
    
    Returns next departures sorted chronologically within the specified time window.
    Can use either time_window_hours from current time or explicit start/end times.
    Resource limits: Maximum 50 departures, 24-hour time window.
    """
    # Add cache headers for client-side caching - shorter cache for departure times
    cache_headers = get_cache_headers(60)  # 1 minute
    for key, value in cache_headers.items():
        response.headers[key] = value
    
    if start_time or end_time:
        try:
            return get_stop_departures_by_time(db, stop_id, start_time, end_time, limit)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
    else:
        # Use time window from current time
        return get_stop_departures_handler(db, stop_id, limit, time_window_hours)
