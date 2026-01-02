from typing import List

from database_connector import DatabaseConnector
from pydantic_models import Stop
from utils.caching import cached


@cached(ttl=600)
def get_route_stops(db: DatabaseConnector, route_id: str) -> List[Stop]:
    """
    Get all stops served by a specific route.

    Args:
        db: Database connector instance
        route_id: Unique identifier for the route

    Returns:
        List of Stop objects in route order
    """
    query = """
            SELECT DISTINCT
                s.stop_id,
                s.stop_name,
                s.stop_lat,
                s.stop_lon,
                s.location_type,
                MIN(st.stop_sequence) as min_sequence
            FROM stops s
                     JOIN stop_times st ON s.stop_id = st.stop_id
                     JOIN trips t ON st.trip_id = t.trip_id
            WHERE t.route_id = ?
            GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon, s.location_type
            ORDER BY min_sequence \
            """

    df = db.execute_df(query, [route_id])

    stops = []
    for _, row in df.iterrows():
        
        location_type = 0
        if row.get('location_type') and str(row['location_type']).strip():
            try:
                location_type = int(row['location_type'])
            except (ValueError, TypeError):
                location_type = 0

        stop = Stop(
            stop_id=row['stop_id'],
            stop_name=row['stop_name'],
            stop_lat=float(row['stop_lat']),
            stop_lon=float(row['stop_lon']),
            location_type=location_type,
            wheelchair_boarding=0,  
            platform_code=None,    
            stop_desc=None,        
            zone_id=None          
        )
        stops.append(stop)

    return stops