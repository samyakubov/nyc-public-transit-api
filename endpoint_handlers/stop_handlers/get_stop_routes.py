from typing import List

from fastapi import HTTPException

from database_connector import DatabaseConnector
from models.pydantic_models import RouteBasic
from utils.caching import cached


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
                ORDER BY r.route_short_name, r.route_long_name \
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
