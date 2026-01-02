from typing import Optional

from database_connector import DatabaseConnector
from endpoint_handlers.route_handlers.get_route_stops import get_route_stops
from models.pydantic_models import RouteDetail
from utils.caching import cached


@cached(ttl=600)
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
                  WHERE route_id = ? \
                  """

    route_df = db.execute_df(route_query, [route_id])

    if route_df.empty:
        return None

    route_row = route_df.iloc[0]

    route_type = 3
    if route_row.get('route_type') and str(route_row['route_type']).strip():
        try:
            route_type = int(route_row['route_type'])
        except (ValueError, TypeError):
            route_type = 3

    # Get stops served by this route
    stops = get_route_stops(db, route_id)



    route_detail = RouteDetail(
        route_id=route_row['route_id'],
        route_short_name=route_row['route_short_name'],
        route_long_name=route_row['route_long_name'],
        route_color=route_row['route_color'],
        route_text_color=route_row['route_text_color'],
        route_type=route_type,
        route_desc=route_row.get('route_desc'),
        stops=stops,
    )

    return route_detail