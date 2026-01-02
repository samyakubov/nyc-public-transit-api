from database_connector import DatabaseConnector
from pydantic_models import Stop
from utils.caching import cached
from utils.error_handling import error_handler
from utils.validation import validate_gtfs_id

@cached(ttl=600)
def get_stop_by_id_handler(db: DatabaseConnector, stop_id: str) -> Stop:
    """
    Get detailed stop information by ID.

    Returns complete stop information including location and accessibility features.
    """
    try:
        validate_gtfs_id(stop_id, "stop_id")

        query = """
                SELECT
                    stop_id,
                    stop_name,
                    CAST(stop_lat AS DOUBLE) as stop_lat,
                    CAST(stop_lon AS DOUBLE) as stop_lon,
                    COALESCE(CAST(location_type AS INTEGER), 0) as location_type
                FROM stops
                WHERE stop_id = ? \
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
