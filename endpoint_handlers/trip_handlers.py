"""
Trip handler functions for the transit API.
Implements trip detail retrieval, stop sequences, active trip information,
and time-based filtering and sorting functionality.
"""

from typing import List, Optional
from database_connector import DatabaseConnector
from pydantic_models import Trip, TripStop, StopDeparture
from datetime import datetime, time, timedelta
import re


def get_trip_by_id(db: DatabaseConnector, trip_id: str) -> Optional[Trip]:
    """
    Get detailed information about a specific trip.
    
    Args:
        db: Database connector instance
        trip_id: Unique identifier for the trip
        
    Returns:
        Trip object or None if not found
    """
    query = """
    SELECT 
        trip_id,
        route_id,
        service_id,
        trip_headsign,
        direction_id,
        shape_id
    FROM trips
    WHERE trip_id = ?
    """
    
    df = db.execute_df(query, [trip_id])
    
    if df.empty:
        return None
    
    row = df.iloc[0]
    
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
    
    return trip


def get_trip_stops(db: DatabaseConnector, trip_id: str) -> List[TripStop]:
    """
    Get the complete stop sequence for a specific trip.
    
    Args:
        db: Database connector instance
        trip_id: Unique identifier for the trip
        
    Returns:
        List of TripStop objects ordered by stop sequence
    """
    query = """
    SELECT 
        st.stop_id,
        s.stop_name,
        st.arrival_time,
        st.departure_time,
        st.stop_sequence
    FROM stop_times st
    JOIN stops s ON st.stop_id = s.stop_id
    WHERE st.trip_id = ?
    ORDER BY st.stop_sequence
    """
    
    df = db.execute_df(query, [trip_id])
    
    trip_stops = []
    for _, row in df.iterrows():
        trip_stop = TripStop(
            stop_id=row['stop_id'],
            stop_name=row['stop_name'],
            arrival_time=row['arrival_time'],
            departure_time=row['departure_time'],
            stop_sequence=int(row['stop_sequence'])
        )
        trip_stops.append(trip_stop)
    
    return trip_stops


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
    # Get current time to simulate "active" trips
    current_time = datetime.now().time()
    
    # Define typical service hours (6 AM to 11 PM)
    service_start = time(6, 0)
    service_end = time(23, 0)
    
    # Base query for trips
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
    WHERE 1=1
    """
    
    params = []
    
    # Add route filter if specified
    if route_id:
        base_query += " AND t.route_id = ?"
        params.append(route_id)
    
    # Simulate active trips by filtering based on departure times
    # This is a simplified approach without real-time data
    if service_start <= current_time <= service_end:
        # During service hours, show trips with departures around current time
        current_time_str = current_time.strftime("%H:%M:%S")
        base_query += """
        AND EXISTS (
            SELECT 1 FROM stop_times st2 
            WHERE st2.trip_id = t.trip_id 
            AND st2.departure_time BETWEEN ? AND ?
        )
        """
        # Show trips departing within the next 2 hours
        end_time = datetime.combine(datetime.today(), current_time)
        end_time = end_time.replace(hour=min(23, end_time.hour + 2))
        params.extend([current_time_str, end_time.strftime("%H:%M:%S")])
    else:
        # Outside service hours, return empty list or early morning trips
        if current_time < service_start:
            # Early morning - show first trips of the day
            base_query += """
            AND EXISTS (
                SELECT 1 FROM stop_times st2 
                WHERE st2.trip_id = t.trip_id 
                AND st2.departure_time BETWEEN '06:00:00' AND '08:00:00'
            )
            """
        else:
            # Late night - return empty list
            return []
    
    base_query += " ORDER BY t.trip_headsign LIMIT ?"
    params.append(limit)
    
    df = db.execute_df(base_query, params)
    
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
    WHERE st.departure_time BETWEEN ? AND ?
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
    # Set default time range if not provided
    if not start_time:
        start_time = datetime.now().strftime("%H:%M:%S")
    
    if not end_time:
        # Default to 2 hours from start time
        start_dt = datetime.strptime(start_time, "%H:%M:%S")
        end_dt = start_dt + timedelta(hours=2)
        # Handle day overflow
        if end_dt.day > start_dt.day:
            end_time = "23:59:59"
        else:
            end_time = end_dt.strftime("%H:%M:%S")
    
    # Validate time format
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
    LIMIT ?
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
            delay_minutes=0  # No real-time data available
        )
        departures.append(departure)
    
    return departures