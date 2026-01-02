from typing import List, Optional
from database_connector import DatabaseConnector
from models.pydantic_models import Trip
import re


def get_trips_by_time_range(
        db: DatabaseConnector,
        start_time: str,
        end_time: str,
        route_id: Optional[str] = None,
        limit: int = 100
) -> List[Trip]:
    """
    Get trips that operate within a specific time range.

    Args:
        db: Database connector instance
        start_time: Start time in HH:MM:SS format
        end_time: End time in HH:MM:SS format
        route_id: Optional filter by route ID
        limit: Maximum number of trips to return

    Returns:
        List of Trip objects that have departures within the time range
    """
    # Validate time format
    time_pattern = r'^\d{1,2}:\d{2}:\d{2}$'
    if not re.match(time_pattern, start_time) or not re.match(time_pattern, end_time):
        raise ValueError("Time must be in HH:MM:SS format")

    query = """
            SELECT DISTINCT
                t.trip_id,
                t.route_id,
                t.service_id,
                t.trip_headsign,
                t.direction_id,
                t.shape_id,
                MIN(st.departure_time) as first_departure
            FROM trips t
                     JOIN stop_times st ON t.trip_id = st.trip_id
            WHERE st.departure_time BETWEEN ? AND ? \
            """

    params = [start_time, end_time]

    if route_id:
        query += " AND t.route_id = ?"
        params.append(route_id)

    query += """
    GROUP BY t.trip_id, t.route_id, t.service_id, t.trip_headsign, t.direction_id, t.shape_id
    ORDER BY first_departure
    LIMIT ?
    """
    params.append(limit)

    df = db.execute_df(query, params)

    trips = []
    for _, row in df.iterrows():
        # Convert direction_id to int if present
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