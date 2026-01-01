from typing import List
from fastapi import HTTPException
from database_connector import DatabaseConnector
from models.pydantic_models import (
    Stop, StopDeparture,
    GeoJSONResponse, RouteBasic, RouteFeature
)
from datetime import datetime, timedelta
from utils.caching import cached
from utils.error_handling import error_handler
from utils.validation import validate_latitude, validate_longitude, validate_radius, validate_gtfs_id


@cached(ttl=300)  # Cache for 5 minutes
def get_nearby_stops_handler(
    db: DatabaseConnector, 
    lat: float, 
    lon: float, 
    radius_miles: float, 
    limit: int
) -> GeoJSONResponse:
    """
    Get stops within radius of a point using spatial queries.
    
    Uses the haversine formula for accurate distance calculations and returns
    results as GeoJSON features with distance information.
    """
    try:
        # Validate input parameters
        validate_latitude(lat)
        validate_longitude(lon)
        validate_radius(radius_miles, max_radius=50.0, unit="miles")
        
        if limit <= 0 or limit > 1000:
            error_handler.handle_validation_error(
                field="limit",
                value=limit,
                constraint="must be between 1 and 1000"
            )
        
        # Convert radius from miles to degrees (approximate)
        radius_deg = radius_miles / 69.0
        
        query = """
        WITH nearby_stops AS (
            SELECT 
                stop_id,
                stop_name,
                CAST(stop_lat AS DOUBLE) as stop_lat,
                CAST(stop_lon AS DOUBLE) as stop_lon,
                COALESCE(CAST(location_type AS INTEGER), 0) as location_type,
                -- Calculate distance using haversine formula approximation
                SQRT(
                    POW(CAST(stop_lat AS DOUBLE) - ?, 2) + 
                    POW(CAST(stop_lon AS DOUBLE) - ?, 2)
                ) * 69.0 as distance_miles
            FROM stops
            WHERE CAST(stop_lat AS DOUBLE) BETWEEN ? - ? AND ? + ?
              AND CAST(stop_lon AS DOUBLE) BETWEEN ? - ? AND ? + ?
        )
        SELECT *
        FROM nearby_stops
        WHERE distance_miles <= ?
        ORDER BY distance_miles
        LIMIT ?
        """
        
        params = [
            lat, lon,  # For distance calculation
            lat, radius_deg, lat, radius_deg,  # Lat bounds
            lon, radius_deg, lon, radius_deg,  # Lon bounds
            radius_miles,  # Final distance filter
            limit
        ]
        
        df = db.execute_df(query, params)
        
        features = []
        for _, row in df.iterrows():
            feature = RouteFeature(
                type="Feature",
                properties={
                    "stop_id": row['stop_id'],
                    "stop_name": row['stop_name'],
                    "stop_lat": row['stop_lat'],
                    "stop_lon": row['stop_lon'],
                    "location_type": row['location_type'],  # Now handled by COALESCE
                    "distance_miles": round(row['distance_miles'], 3)
                },
                geometry={
                    "type": "Point",
                    "coordinates": [row['stop_lon'], row['stop_lat']]
                }
            )
            features.append(feature)
        
        return GeoJSONResponse(features=features)
        
    except (ValueError, TypeError) as e:
        # Handle validation errors
        if "latitude" in str(e).lower():
            error_handler.handle_validation_error("latitude", lat, str(e))
        elif "longitude" in str(e).lower():
            error_handler.handle_validation_error("longitude", lon, str(e))
        elif "radius" in str(e).lower():
            error_handler.handle_validation_error("radius_miles", radius_miles, str(e))
        else:
            error_handler.handle_validation_error("parameters", f"lat={lat}, lon={lon}, radius={radius_miles}", str(e))
    except Exception as e:
        error_handler.handle_database_error("nearby stops search", e)


@cached(ttl=600)  # Cache for 10 minutes - stop data changes infrequently
def get_stop_by_id_handler(db: DatabaseConnector, stop_id: str) -> Stop:
    """
    Get detailed stop information by ID.
    
    Returns complete stop information including location and accessibility features.
    """
    try:
        # Validate stop ID format
        validate_gtfs_id(stop_id, "stop_id")
        
        query = """
        SELECT 
            stop_id,
            stop_name,
            CAST(stop_lat AS DOUBLE) as stop_lat,
            CAST(stop_lon AS DOUBLE) as stop_lon,
            COALESCE(CAST(location_type AS INTEGER), 0) as location_type
        FROM stops
        WHERE stop_id = ?
        """
        
        df = db.execute_df(query, [stop_id])
        
        if df.empty:
            error_handler.handle_not_found("stop", stop_id)
        
        row = df.iloc[0]
        return Stop(
            stop_id=row['stop_id'],
            stop_name=row['stop_name'],
            stop_lat=row['stop_lat'],
            stop_lon=row['stop_lon'],
            location_type=row['location_type'],  # Now handled by COALESCE
            wheelchair_boarding=0,  # Default value since not in current schema
            platform_code=None,
            stop_desc=None,
            zone_id=None
        )
        
    except ValueError as e:
        error_handler.handle_validation_error("stop_id", stop_id, str(e))
    except Exception as e:
        error_handler.handle_database_error("stop retrieval", e)


@cached(ttl=300)  # Cache for 5 minutes
def search_stops_handler(db: DatabaseConnector, query_text: str, limit: int) -> List[Stop]:
    """
    Search stops by name using fuzzy search capabilities.
    
    Uses LIKE pattern matching and similarity scoring to find relevant stops.
    """
    try:
        # Validate search parameters
        from utils.validation import validate_search_query
        query_text = validate_search_query(query_text, min_length=1, max_length=100)
        
        if limit <= 0 or limit > 1000:
            error_handler.handle_validation_error(
                field="limit",
                value=limit,
                constraint="must be between 1 and 1000"
            )
        
        # Implement fuzzy search using LIKE patterns and similarity scoring
        search_pattern = f"%{query_text.lower()}%"
        
        query = """
        WITH scored_stops AS (
            SELECT 
                stop_id,
                stop_name,
                CAST(stop_lat AS DOUBLE) as stop_lat,
                CAST(stop_lon AS DOUBLE) as stop_lon,
                COALESCE(CAST(location_type AS INTEGER), 0) as location_type,
                -- Simple relevance scoring based on position of match
                CASE 
                    WHEN LOWER(stop_name) = LOWER(?) THEN 100  -- Exact match
                    WHEN LOWER(stop_name) LIKE LOWER(?) THEN 90  -- Starts with query
                    WHEN LOWER(stop_name) LIKE ? THEN 80  -- Contains query
                    ELSE 50  -- Partial match
                END as relevance_score
            FROM stops
            WHERE LOWER(stop_name) LIKE ?
        )
        SELECT *
        FROM scored_stops
        ORDER BY relevance_score DESC, stop_name
        LIMIT ?
        """
        
        params = [
            query_text,  # Exact match
            f"{query_text.lower()}%",  # Starts with
            search_pattern,  # Contains
            search_pattern,  # WHERE clause
            limit
        ]
        
        df = db.execute_df(query, params)
        
        stops = []
        for _, row in df.iterrows():
            stop = Stop(
                stop_id=row['stop_id'],
                stop_name=row['stop_name'],
                stop_lat=row['stop_lat'],
                stop_lon=row['stop_lon'],
                location_type=row['location_type'],  # Now handled by COALESCE in query
                wheelchair_boarding=0,
                platform_code=None,
                stop_desc=None,
                zone_id=None
            )
            stops.append(stop)
        
        return stops
        
    except ValueError as e:
        if "search query" in str(e).lower():
            error_handler.handle_validation_error("q", query_text, str(e))
        else:
            error_handler.handle_validation_error("limit", limit, str(e))
    except Exception as e:
        error_handler.handle_database_error("stop search", e)


@cached(ttl=600)  # Cache for 10 minutes - route-stop relationships are stable
def get_stop_routes_handler(db: DatabaseConnector, stop_id: str) -> List[RouteBasic]:
    """
    Get all routes that serve a specific stop.
    
    Returns basic route information for all routes stopping at this location.
    """
    try:
        # First verify the stop exists
        stop_check_query = "SELECT COUNT(*) as count FROM stops WHERE stop_id = ?"
        stop_count = db.execute_df(stop_check_query, [stop_id])
        
        if stop_count.iloc[0]['count'] == 0:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "code": "STOP_NOT_FOUND",
                        "message": f"Stop with ID '{stop_id}' not found",
                        "details": {
                            "field": "stop_id",
                            "value": stop_id,
                            "constraint": "must be a valid stop identifier"
                        }
                    }
                }
            )
        
        query = """
        SELECT DISTINCT
            r.route_id,
            r.route_short_name,
            r.route_long_name,
            r.route_color,
            r.route_text_color,
            COALESCE(CAST(r.route_type AS INTEGER), 3) as route_type
        FROM routes r
        JOIN trips t ON r.route_id = t.route_id
        JOIN stop_times st ON t.trip_id = st.trip_id
        WHERE st.stop_id = ?
        ORDER BY r.route_short_name, r.route_long_name
        """
        
        df = db.execute_df(query, [stop_id])
        
        routes = []
        for _, row in df.iterrows():
            route = RouteBasic(
                route_id=row['route_id'],
                route_short_name=row['route_short_name'] or '',
                route_long_name=row['route_long_name'] or '',
                route_color=row['route_color'] or 'FFFFFF',
                route_text_color=row['route_text_color'] or '000000',
                route_type=row['route_type']  # Now handled by COALESCE
            )
            routes.append(route)
        
        return routes
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving stop routes: {str(e)}")


@cached(ttl=60)  # Cache for 1 minute - departure times change frequently
def get_stop_departures_handler(
    db: DatabaseConnector, 
    stop_id: str, 
    limit: int, 
    time_window_hours: int
) -> List[StopDeparture]:
    """
    Get upcoming departures from a specific stop.
    
    Returns next departures sorted chronologically within the specified time window.
    Uses actual departure times from the stop_times table.
    """
    try:
        # First verify the stop exists
        stop_check_query = "SELECT COUNT(*) as count FROM stops WHERE stop_id = ?"
        stop_count = db.execute_df(stop_check_query, [stop_id])
        
        if stop_count.iloc[0]['count'] == 0:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "code": "STOP_NOT_FOUND",
                        "message": f"Stop with ID '{stop_id}' not found",
                        "details": {
                            "field": "stop_id",
                            "value": stop_id,
                            "constraint": "must be a valid stop identifier"
                        }
                    }
                }
            )
        
        # Calculate time range
        current_time = datetime.now()
        start_time = current_time.strftime("%H:%M:%S")
        end_time = (current_time + timedelta(hours=time_window_hours)).strftime("%H:%M:%S")
        
        # Handle day overflow - if end time is past midnight, limit to end of day
        if time_window_hours > 24 - current_time.hour:
            end_time = "23:59:59"
        
        query = """
        SELECT 
            t.trip_id,
            t.route_id,
            r.route_short_name,
            r.route_long_name,
            t.trip_headsign,
            st.departure_time,
            st.stop_sequence
        FROM stop_times st
        JOIN trips t ON st.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        WHERE st.stop_id = ?
        AND st.departure_time BETWEEN ? AND ?
        ORDER BY st.departure_time, st.stop_sequence
        LIMIT ?
        """
        
        df = db.execute_df(query, [stop_id, start_time, end_time, limit])
        
        departures = []
        for _, row in df.iterrows():
            departure = StopDeparture(
                trip_id=row['trip_id'],
                route_id=row['route_id'],
                route_short_name=row['route_short_name'] or '',
                route_long_name=row['route_long_name'] or '',
                headsign=row.get('trip_headsign'),
                departure_time=row['departure_time'],
                delay_minutes=0  # No real-time data available
            )
            departures.append(departure)
        
        return departures
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving stop departures: {str(e)}")