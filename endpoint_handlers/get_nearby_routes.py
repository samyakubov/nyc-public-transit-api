from database_connector import  DatabaseConnector
from models.pydantic_models import GeoJSONResponse
from utils.caching import cached


@cached(ttl=300)  # Cache for 5 minutes
def get_nearby_routes(db: DatabaseConnector, lat: float, lon: float, radius_miles: float) -> GeoJSONResponse:
    """Get routes within radius of a point"""

    radius_deg = radius_miles / 69.0

    query = """
            WITH nearby_shapes AS (
                SELECT DISTINCT
                    t.route_id,
                    t.shape_id,
                    MIN(SQRT(
                            POW(CAST(s.shape_pt_lon AS DOUBLE) - ?, 2) +
                            POW(CAST(s.shape_pt_lat AS DOUBLE) - ?, 2)
                        )) as min_distance
                FROM shapes s
                         JOIN trips t ON s.shape_id = t.shape_id
                WHERE CAST(s.shape_pt_lat AS DOUBLE) BETWEEN ? - ? AND ? + ?
                  AND CAST(s.shape_pt_lon AS DOUBLE) BETWEEN ? - ? AND ? + ?
                GROUP BY t.route_id, t.shape_id
                HAVING min_distance < ?
            ),
                 route_coords AS (
                     SELECT
                         ns.route_id,
                         ns.shape_id,
                         ns.min_distance,
                         LIST(STRUCT_PACK(
                                 lat := CAST(s.shape_pt_lat AS DOUBLE),
                                 lon := CAST(s.shape_pt_lon AS DOUBLE)
                              ) ORDER BY s.shape_pt_sequence) as coordinates
                     FROM nearby_shapes ns
                              JOIN shapes s ON ns.shape_id = s.shape_id
                     GROUP BY ns.route_id, ns.shape_id, ns.min_distance
                 )
            SELECT
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_color,
                r.route_text_color,
                rc.shape_id,
                rc.coordinates,
                rc.min_distance
            FROM route_coords rc
                     JOIN routes r ON rc.route_id = r.route_id
            ORDER BY rc.min_distance
                LIMIT 20 \
            """

    params = [lon, lat, lat, radius_deg, lat, radius_deg, lon, radius_deg, lon, radius_deg, radius_deg]

    df = db.execute_df(query, params)

    features = []
    for _, row in df.iterrows():
        coords = [[c['lon'], c['lat']] for c in row['coordinates']]
        feature = {
            "type": "Feature",
            "properties": {
                "route_id": row['route_id'],
                "route_name": row['route_short_name'],
                "route_long_name": row['route_long_name'],
                "route_color": f"#{row['route_color']}" if row['route_color'] else "#0039A6",
                "route_text_color": f"#{row['route_text_color']}" if row['route_text_color'] else "#FFFFFF",
                "shape_id": row['shape_id'],
                "distance": round(row['min_distance'] * 69, 2)
            },
            "geometry": {
                "type": "LineString",
                "coordinates": coords
            }
        }
        features.append(feature)

    return GeoJSONResponse(features=features)