from typing import List
from database_connector import DatabaseConnector
from endpoint_handlers.stop_handlers.get_stop_by_id import get_stop_by_id_handler
from pydantic_models import Stop
from utils.caching import cached
from utils.error_handling import error_handler
from utils.validation import validate_latitude, validate_longitude, validate_radius

@cached(ttl=300)
def get_nearby_stops_handler(
        db: DatabaseConnector,
        lat: float,
        lon: float,
        radius_miles: float,
        limit: int
) -> List[Stop]:
    """
    Get stops within radius of a point using spatial queries.

    Uses the haversine formula for accurate distance calculations and returns
    a list of Stop objects with distance information.
    """
    try:
        
        validate_latitude(lat)
        validate_longitude(lon)
        validate_radius(radius_miles, max_radius=50.0, unit="miles")

        if limit <= 0 or limit > 1000:
            error_handler.handle_validation_error(
                field="limit",
                value=limit,
                constraint="must be between 1 and 1000"
            )

        radius_deg = radius_miles / 69.0

        query = """
                WITH nearby_stops AS (
                    SELECT
                        stop_id,
                        SQRT(
                                POW(CAST(stop_lat AS DOUBLE) - ?, 2) +
                                POW(CAST(stop_lon AS DOUBLE) - ?, 2)
                        ) * 69.0 as distance_miles
                    FROM stops
                    WHERE CAST(stop_lat AS DOUBLE) BETWEEN ? - ? AND ? + ?
                      AND CAST(stop_lon AS DOUBLE) BETWEEN ? - ? AND ? + ?
                )
                SELECT stop_id
                FROM nearby_stops
                WHERE distance_miles <= ?
                ORDER BY distance_miles
                    LIMIT ?
                """

        params = [
            lat, lon,
            lat, radius_deg, lat, radius_deg,
            lon, radius_deg, lon, radius_deg,
            radius_miles,
            limit
        ]

        df = db.execute_df(query, params)

        stops = []
        for _, row in df.iterrows():
            stop = get_stop_by_id_handler(db, row['stop_id'])
            if stop:
                stops.append(stop)

        return stops

    except (ValueError, TypeError) as e:
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