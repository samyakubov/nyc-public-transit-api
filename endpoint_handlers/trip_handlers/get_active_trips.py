from typing import List, Optional
from database_connector import DatabaseConnector
from pydantic_models import Trip
from datetime import datetime, time

def get_active_trips(db: DatabaseConnector, route_id: Optional[str] = None, limit: int = 100) -> List[Trip]:
    """
    Get currently active trips. Since we don't have real-time data,
    this returns trips that would be active during typical service hours.

    Args:
        db: Database connector instance
        route_id: Optional filter by route ID
        limit: Maximum number of trips to return

    Returns:
        List of Trip objects
    """
    
    current_time = datetime.now().time()

    
    service_start = time(6, 0)
    service_end = time(23, 0)

    
    base_query = """
                 SELECT DISTINCT
                     t.trip_id,
                     t.route_id,
                     t.service_id,
                     t.trip_headsign,
                     t.direction_id,
                     t.shape_id
                 FROM trips t
                          JOIN stop_times st ON t.trip_id = st.trip_id
                 WHERE 1=1 \
                 """

    params = []

    
    if route_id:
        base_query += " AND t.route_id = ?"
        params.append(route_id)

    
    
    if service_start <= current_time <= service_end:
        
        current_time_str = current_time.strftime("%H:%M:%S")
        base_query += """
        AND EXISTS (
            SELECT 1 FROM stop_times st2 
            WHERE st2.trip_id = t.trip_id 
            AND st2.departure_time BETWEEN ? AND ?
        )
        """
        
        end_time = datetime.combine(datetime.today(), current_time)
        end_time = end_time.replace(hour=min(23, end_time.hour + 2))
        params.extend([current_time_str, end_time.strftime("%H:%M:%S")])
    else:
        
        if current_time < service_start:
            
            base_query += """
            AND EXISTS (
                SELECT 1 FROM stop_times st2 
                WHERE st2.trip_id = t.trip_id 
                AND st2.departure_time BETWEEN '06:00:00' AND '08:00:00'
            )
            """
        else:
            
            return []

    base_query += " ORDER BY t.trip_headsign LIMIT ?"
    params.append(limit)

    df = db.execute_df(base_query, params)

    trips = []
    for _, row in df.iterrows():
        
        direction_id = None
        if row.get('direction_id') and str(row['direction_id']).strip():
            try:
                direction_id = int(row['direction_id'])
            except (ValueError, TypeError):
                direction_id = None

        trip = Trip(
            trip_id=row['trip_id'],
            route_id=row['route_id'],
            service_id=row['service_id'],
            trip_headsign=row.get('trip_headsign'),
            direction_id=direction_id,
            shape_id=row.get('shape_id')
        )
        trips.append(trip)

    return trips