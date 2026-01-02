"""
Trip handler functions for the transit API.
Implements trip detail retrieval, stop sequences, active trip information,
and time-based filtering and sorting functionality.
"""

from typing import List, Optional
from database_connector import DatabaseConnector
from pydantic_models import StopDeparture
from datetime import datetime, timedelta
import re


def get_stop_departures_by_time(
        db: DatabaseConnector,
        stop_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 20
) -> List[StopDeparture]:
    """
    Get departures from a stop within a specific time range, sorted chronologically.

    Args:
        db: Database connector instance
        stop_id: Stop identifier
        start_time: Optional start time in HH:MM:SS format (defaults to current time)
        end_time: Optional end time in HH:MM:SS format (defaults to 2 hours from start)
        limit: Maximum number of departures to return

    Returns:
        List of StopDeparture objects sorted by departure time
    """
    
    if not start_time:
        start_time = datetime.now().strftime("%H:%M:%S")

    if not end_time:
        
        start_dt = datetime.strptime(start_time, "%H:%M:%S")
        end_dt = start_dt + timedelta(hours=2)
        
        if end_dt.day > start_dt.day:
            end_time = "23:59:59"
        else:
            end_time = end_dt.strftime("%H:%M:%S")

    
    time_pattern = r'^\d{1,2}:\d{2}:\d{2}$'
    if not re.match(time_pattern, start_time) or not re.match(time_pattern, end_time):
        raise ValueError("Time must be in HH:MM:SS format")

    query = """
            SELECT
                t.trip_id,
                t.route_id,
                r.route_short_name,
                r.route_long_name,
                t.trip_headsign,
                st.departure_time,
                st.stop_sequence
            FROM stop_times st
                     JOIN trips t ON st.trip_id = t.trip_id
                     JOIN routes r ON t.route_id = r.route_id
            WHERE st.stop_id = ?
              AND st.departure_time BETWEEN ? AND ?
            ORDER BY st.departure_time, st.stop_sequence
                LIMIT ? \
            """

    df = db.execute_df(query, [stop_id, start_time, end_time, limit])

    departures = []
    for _, row in df.iterrows():
        departure = StopDeparture(
            trip_id=row['trip_id'],
            route_id=row['route_id'],
            route_short_name=row['route_short_name'] or '',
            route_long_name=row['route_long_name'] or '',
            headsign=row.get('trip_headsign'),
            departure_time=row['departure_time'],
        )
        departures.append(departure)

    return departures