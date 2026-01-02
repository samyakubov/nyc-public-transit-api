from typing import List

from database_connector import DatabaseConnector
from pydantic_models import Stop
from utils.caching import cached
from utils.error_handling import error_handler


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
                    LIMIT ? \
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