from typing import List
from database_connector import DatabaseConnector
from endpoint_handlers.route_handlers.get_route_by_id import get_route_by_id
from pydantic_models import RouteDetail
from utils.caching import cached

@cached(ttl=300)
def get_nearby_routes(db: DatabaseConnector, lat: float, lon: float, radius_miles: float) -> List[RouteDetail]:
    """Get routes within radius of a point"""

    radius_deg = radius_miles / 69.0

    query = """
            WITH nearby_shapes AS (
                SELECT DISTINCT
                    t.route_id,
                    MIN(SQRT(
                            POW(CAST(s.shape_pt_lon AS DOUBLE) - ?, 2) +
                            POW(CAST(s.shape_pt_lat AS DOUBLE) - ?, 2)
                        )) as min_distance
                FROM shapes s
                         JOIN trips t ON s.shape_id = t.shape_id
                WHERE CAST(s.shape_pt_lat AS DOUBLE) BETWEEN ? - ? AND ? + ?
                  AND CAST(s.shape_pt_lon AS DOUBLE) BETWEEN ? - ? AND ? + ?
                GROUP BY t.route_id
                HAVING min_distance < ?
            )
            SELECT route_id
            FROM nearby_shapes
            ORDER BY min_distance
                LIMIT 20
            """

    params = [lon, lat, lat, radius_deg, lat, radius_deg, lon, radius_deg, lon, radius_deg, radius_deg]

    df = db.execute_df(query, params)

    routes = []
    for _, row in df.iterrows():
        route_detail = get_route_by_id(db, row['route_id'])
        if route_detail:
            routes.append(route_detail)

    return routes