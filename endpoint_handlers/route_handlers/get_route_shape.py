from typing import Optional
from database_connector import DatabaseConnector
from pydantic_models import GeoJSONResponse
from utils.caching import cached

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
            GROUP BY t.shape_id \
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
                       WHERE route_id = ? \
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