from typing import List

from database_connector import DatabaseConnector
from models.pydantic_models import RouteBasic
from utils.caching import cached


@cached(ttl=600)
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
                LIMIT ? OFFSET ? \
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