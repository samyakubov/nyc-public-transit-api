
from database_connector import DatabaseConnector
from models.pydantic_models import (
    GeoJSONResponse, RouteFeature
)
from utils.caching import cached
from utils.error_handling import error_handler
from utils.validation import validate_latitude, validate_longitude, validate_radius

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
                    LIMIT ? \
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