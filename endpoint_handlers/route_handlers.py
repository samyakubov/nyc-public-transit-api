"""
Route handler functions for the transit API.
Implements route listing, detail retrieval, route-stop relationships, and route shape geometry.
"""

from typing import List, Optional, Dict, Any
from database_connector import DatabaseConnector
from pydantic_models import RouteBasic, RouteDetail, Stop, Trip, GeoJSONResponse
from utils.caching import cached


@cached(ttl=600)  # Cache for 10 minutes - route list changes infrequently
def get_all_routes(db: DatabaseConnector, limit: int = 100, offset: int = 0) -> List[RouteBasic]:
    """
    Get a list of all available routes with basic information.
    
    Args:
        db: Database connector instance
        limit: Maximum number of routes to return
        offset: Number of routes to skip
        
    Returns:
        List of RouteBasic objects
    """
    query = """
    SELECT 
        route_id,
        route_short_name,
        route_long_name,
        COALESCE(route_color, 'FFFFFF') as route_color,
        COALESCE(route_text_color, '000000') as route_text_color,
        route_type
    FROM routes
    ORDER BY route_short_name, route_long_name
    LIMIT ? OFFSET ?
    """
    
    df = db.execute_df(query, [limit, offset])
    
    routes = []
    for _, row in df.iterrows():
        # Convert route_type to int, defaulting to 3 (bus) if empty or invalid
        route_type = 3
        if row.get('route_type') and str(row['route_type']).strip():
            try:
                route_type = int(row['route_type'])
            except (ValueError, TypeError):
                route_type = 3
        
        route = RouteBasic(
            route_id=row['route_id'],
            route_short_name=row['route_short_name'],
            route_long_name=row['route_long_name'],
            route_color=row['route_color'],
            route_text_color=row['route_text_color'],
            route_type=route_type
        )
        routes.append(route)
    
    return routes


@cached(ttl=600)  # Cache for 10 minutes - route details are stable
def get_route_by_id(db: DatabaseConnector, route_id: str) -> Optional[RouteDetail]:
    """
    Get detailed information about a specific route.
    
    Args:
        db: Database connector instance
        route_id: Unique identifier for the route
        
    Returns:
        RouteDetail object or None if not found
    """
    # Get basic route information
    route_query = """
    SELECT 
        route_id,
        route_short_name,
        route_long_name,
        route_desc,
        COALESCE(route_color, 'FFFFFF') as route_color,
        COALESCE(route_text_color, '000000') as route_text_color,
        route_type
    FROM routes
    WHERE route_id = ?
    """
    
    route_df = db.execute_df(route_query, [route_id])
    
    if route_df.empty:
        return None
    
    route_row = route_df.iloc[0]
    
    # Convert route_type to int, defaulting to 3 (bus) if empty or invalid
    route_type = 3
    if route_row.get('route_type') and str(route_row['route_type']).strip():
        try:
            route_type = int(route_row['route_type'])
        except (ValueError, TypeError):
            route_type = 3
    
    # Get stops served by this route
    stops = get_route_stops(db, route_id)
    
    # Get service days for this route
    service_days = get_route_service_days(db, route_id)
    
    route_detail = RouteDetail(
        route_id=route_row['route_id'],
        route_short_name=route_row['route_short_name'],
        route_long_name=route_row['route_long_name'],
        route_color=route_row['route_color'],
        route_text_color=route_row['route_text_color'],
        route_type=route_type,
        route_desc=route_row.get('route_desc'),
        stops=stops,
        service_days=service_days
    )
    
    return route_detail


@cached(ttl=600)  # Cache for 10 minutes - route stops are stable
def get_route_stops(db: DatabaseConnector, route_id: str) -> List[Stop]:
    """
    Get all stops served by a specific route.
    
    Args:
        db: Database connector instance
        route_id: Unique identifier for the route
        
    Returns:
        List of Stop objects in route order
    """
    query = """
    SELECT DISTINCT
        s.stop_id,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        s.location_type,
        MIN(st.stop_sequence) as min_sequence
    FROM stops s
    JOIN stop_times st ON s.stop_id = st.stop_id
    JOIN trips t ON st.trip_id = t.trip_id
    WHERE t.route_id = ?
    GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon, s.location_type
    ORDER BY min_sequence
    """
    
    df = db.execute_df(query, [route_id])
    
    stops = []
    for _, row in df.iterrows():
        # Convert location_type to int, defaulting to 0 if empty or null
        location_type = 0
        if row.get('location_type') and str(row['location_type']).strip():
            try:
                location_type = int(row['location_type'])
            except (ValueError, TypeError):
                location_type = 0
        
        stop = Stop(
            stop_id=row['stop_id'],
            stop_name=row['stop_name'],
            stop_lat=float(row['stop_lat']),
            stop_lon=float(row['stop_lon']),
            location_type=location_type,
            wheelchair_boarding=0,  # Default since not in schema
            platform_code=None,    # Default since not in schema
            stop_desc=None,        # Default since not in schema
            zone_id=None          # Default since not in schema
        )
        stops.append(stop)
    
    return stops


@cached(ttl=300)  # Cache for 5 minutes - trip schedules can change
def get_route_trips(db: DatabaseConnector, route_id: str, service_date: Optional[str] = None, limit: int = 100) -> List[Trip]:
    """
    Get all trips for a specific route.
    
    Args:
        db: Database connector instance
        route_id: Unique identifier for the route
        service_date: Optional service date filter in YYYY-MM-DD format (not used without calendar table)
        limit: Maximum number of trips to return
        
    Returns:
        List of Trip objects
    """
    query = """
    SELECT 
        trip_id,
        route_id,
        service_id,
        trip_headsign,
        direction_id,
        shape_id
    FROM trips
    WHERE route_id = ?
    ORDER BY trip_headsign, direction_id
    LIMIT ?
    """
    params = [route_id, limit]
    
    df = db.execute_df(query, params)
    
    trips = []
    for _, row in df.iterrows():
        # Convert direction_id to int if present
        direction_id = None
        if row.get('direction_id') and str(row['direction_id']).strip():
            try:
                direction_id = int(row['direction_id'])
            except (ValueError, TypeError):
                direction_id = None
        
        trip = Trip(
            trip_id=row['trip_id'],
            route_id=row['route_id'],
            service_id=row['service_id'],
            trip_headsign=row.get('trip_headsign'),
            direction_id=direction_id,
            shape_id=row.get('shape_id')
        )
        trips.append(trip)
    
    return trips


@cached(ttl=1800)  # Cache for 30 minutes - route shapes rarely change
def get_route_shape(db: DatabaseConnector, route_id: str) -> Optional[GeoJSONResponse]:
    """
    Get the geometric shape/path for a specific route.
    
    Args:
        db: Database connector instance
        route_id: Unique identifier for the route
        
    Returns:
        GeoJSONResponse with route shape or None if not found
    """
    query = """
    SELECT DISTINCT
        t.shape_id,
        LIST(STRUCT_PACK(
            lat := CAST(s.shape_pt_lat AS DOUBLE),
            lon := CAST(s.shape_pt_lon AS DOUBLE)
        ) ORDER BY s.shape_pt_sequence) as coordinates
    FROM trips t
    JOIN shapes s ON t.shape_id = s.shape_id
    WHERE t.route_id = ?
    AND t.shape_id IS NOT NULL
    GROUP BY t.shape_id
    """
    
    df = db.execute_df(query, [route_id])
    
    if df.empty:
        return None
    
    # Get route information for properties
    route_info_query = """
    SELECT 
        route_id,
        route_short_name,
        route_long_name,
        COALESCE(route_color, 'FFFFFF') as route_color,
        COALESCE(route_text_color, '000000') as route_text_color
    FROM routes
    WHERE route_id = ?
    """
    
    route_df = db.execute_df(route_info_query, [route_id])
    
    if route_df.empty:
        return None
    
    route_info = route_df.iloc[0]
    
    features = []
    for _, row in df.iterrows():
        coords = [[c['lon'], c['lat']] for c in row['coordinates']]
        feature = {
            "type": "Feature",
            "properties": {
                "route_id": route_info['route_id'],
                "route_name": route_info['route_short_name'],
                "route_long_name": route_info['route_long_name'],
                "route_color": f"#{route_info['route_color']}",
                "route_text_color": f"#{route_info['route_text_color']}",
                "shape_id": row['shape_id']
            },
            "geometry": {
                "type": "LineString",
                "coordinates": coords
            }
        }
        features.append(feature)
    
    return GeoJSONResponse(features=features)


@cached(ttl=3600)  # Cache for 1 hour - service days are very stable
def get_route_service_days(db: DatabaseConnector, route_id: str) -> List[str]:
    """
    Get the service days for a route based on its trips.
    Since calendar table is not available, return a default set of service days.
    
    Args:
        db: Database connector instance
        route_id: Unique identifier for the route
        
    Returns:
        List of service day names
    """
    # Without calendar table, we can't determine actual service days
    # Return a default set indicating weekday service
    return ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]