from fastapi import APIRouter, HTTPException, Query, Depends, Response
from typing import List, Optional
from database_connector import get_db, DatabaseConnector
from pydantic_models import RouteBasic, RouteDetail, Stop, Trip, GeoJSONResponse
from endpoint_handlers.get_nearby_routes import get_nearby_routes
from endpoint_handlers.route_handlers import (
    get_all_routes,
    get_route_by_id,
    get_route_stops,
    get_route_trips,
    get_route_shape
)
from utils.caching import get_cache_headers

route_routes = APIRouter(prefix="/routes")

@route_routes.get("/nearby")
def get_nearby(
    response: Response,
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    radius_miles: float = Query(0.5, description="Search radius in miles"),
    db: DatabaseConnector = Depends(get_db)
):
    # Add cache headers for client-side caching
    cache_headers = get_cache_headers(300)  # 5 minutes
    for key, value in cache_headers.items():
        response.headers[key] = value
    
    return get_nearby_routes(db, lat, lon, radius_miles)

@route_routes.get("/", response_model=List[RouteBasic])
def list_routes(
    response: Response,
    db: DatabaseConnector = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of routes to return"),
    offset: int = Query(0, ge=0, description="Number of routes to skip")
):
    """Get a list of all available routes."""
    # Add cache headers for client-side caching
    cache_headers = get_cache_headers(600)  # 10 minutes
    for key, value in cache_headers.items():
        response.headers[key] = value
    
    return get_all_routes(db, limit, offset)

@route_routes.get("/{route_id}", response_model=RouteDetail)
def get_route(
    response: Response,
    route_id: str,
    db: DatabaseConnector = Depends(get_db)
):
    """Get detailed information about a specific route."""
    # Add cache headers for client-side caching
    cache_headers = get_cache_headers(600)  # 10 minutes
    for key, value in cache_headers.items():
        response.headers[key] = value
    
    route = get_route_by_id(db, route_id)
    if not route:
        raise HTTPException(status_code=404, detail=f"Route {route_id} not found")
    return route

@route_routes.get("/{route_id}/stops", response_model=List[Stop])
def get_route_stops_endpoint(
    response: Response,
    route_id: str,
    db: DatabaseConnector = Depends(get_db)
):
    """Get all stops served by a specific route."""
    # Add cache headers for client-side caching
    cache_headers = get_cache_headers(600)  # 10 minutes
    for key, value in cache_headers.items():
        response.headers[key] = value
    
    stops = get_route_stops(db, route_id)
    if not stops:
        # Check if route exists
        route = get_route_by_id(db, route_id)
        if not route:
            raise HTTPException(status_code=404, detail=f"Route {route_id} not found")
    return stops

@route_routes.get("/{route_id}/trips", response_model=List[Trip])
def get_route_trips_endpoint(
    response: Response,
    route_id: str,
    db: DatabaseConnector = Depends(get_db),
    service_date: Optional[str] = Query(None, description="Service date in YYYY-MM-DD format"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of trips to return")
):
    """Get all trips for a specific route."""
    # Add cache headers for client-side caching
    cache_headers = get_cache_headers(300)  # 5 minutes
    for key, value in cache_headers.items():
        response.headers[key] = value
    
    trips = get_route_trips(db, route_id, service_date, limit)
    if not trips:
        # Check if route exists
        route = get_route_by_id(db, route_id)
        if not route:
            raise HTTPException(status_code=404, detail=f"Route {route_id} not found")
    return trips

@route_routes.get("/{route_id}/shape")
def get_route_shape_endpoint(
    response: Response,
    route_id: str,
    db: DatabaseConnector = Depends(get_db)
):
    """Get the geometric shape/path for a specific route."""
    # Add cache headers for client-side caching - longer cache for shapes
    cache_headers = get_cache_headers(1800)  # 30 minutes
    for key, value in cache_headers.items():
        response.headers[key] = value
    
    shape = get_route_shape(db, route_id)
    if not shape:
        # Check if route exists
        route = get_route_by_id(db, route_id)
        if not route:
            raise HTTPException(status_code=404, detail=f"Route {route_id} not found")
        raise HTTPException(status_code=404, detail=f"No shape data found for route {route_id}")
    return shape


