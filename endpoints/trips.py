from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
from database_connector import get_db, DatabaseConnector
from endpoint_handlers.trip_handlers.get_trip_by_id import get_trip_by_id

from endpoint_handlers.trip_handlers.get_trip_stops import get_trip_stops

from endpoint_handlers.trip_handlers.get_active_trips import get_active_trips

from endpoint_handlers.trip_handlers.get_trips_by_time_range import get_trips_by_time_range

from endpoint_handlers.trip_handlers.get_trip_by_time import get_stop_departures_by_time
from models.pydantic_models import Trip, TripStop, StopDeparture

trip_routes = APIRouter(prefix="/trips")

@trip_routes.get("/{trip_id}", response_model=Trip)
def get_trip(
    trip_id: str,
    db: DatabaseConnector = Depends(get_db)
):
    """Get detailed information about a specific trip."""
    trip = get_trip_by_id(db, trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found")
    return trip

@trip_routes.get("/{trip_id}/stops", response_model=List[TripStop])
def get_trip_stops_endpoint(
    trip_id: str,
    db: DatabaseConnector = Depends(get_db)
):
    """Get the complete stop sequence for a specific trip."""
    stops = get_trip_stops(db, trip_id)
    if not stops:
        # Check if trip exists
        trip = get_trip_by_id(db, trip_id)
        if not trip:
            raise HTTPException(status_code=404, detail=f"Trip {trip_id} not found")
        raise HTTPException(status_code=404, detail=f"No stops found for trip {trip_id}")
    return stops

@trip_routes.get("/active", response_model=List[Trip])
def get_active_trips_endpoint(
    db: DatabaseConnector = Depends(get_db),
    route_id: Optional[str] = Query(None, description="Filter by route ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of trips to return")
):
    """Get currently active trips (simplified implementation without real-time data)."""
    trips = get_active_trips(db, route_id, limit)
    return trips

@trip_routes.get("/by-time", response_model=List[Trip])
def get_trips_by_time_endpoint(
    start_time: str = Query(..., description="Start time in HH:MM:SS format", regex=r'^\d{1,2}:\d{2}:\d{2}$'),
    end_time: str = Query(..., description="End time in HH:MM:SS format", regex=r'^\d{1,2}:\d{2}:\d{2}$'),
    route_id: Optional[str] = Query(None, description="Filter by route ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of trips to return"),
    db: DatabaseConnector = Depends(get_db)
):
    """Get trips that operate within a specific time range."""
    try:
        trips = get_trips_by_time_range(db, start_time, end_time, route_id, limit)
        return trips
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@trip_routes.get("/departures/{stop_id}", response_model=List[StopDeparture])
def get_stop_departures_by_time_endpoint(
    stop_id: str,
    start_time: Optional[str] = Query(None, description="Start time in HH:MM:SS format", regex=r'^\d{1,2}:\d{2}:\d{2}$'),
    end_time: Optional[str] = Query(None, description="End time in HH:MM:SS format", regex=r'^\d{1,2}:\d{2}:\d{2}$'),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of departures to return"),
    db: DatabaseConnector = Depends(get_db)
):
    """Get departures from a stop within a specific time range, sorted chronologically."""
    try:
        departures = get_stop_departures_by_time(db, stop_id, start_time, end_time, limit)
        if not departures:
            check_query = "SELECT COUNT(*) as count FROM stop_times WHERE stop_id = ?"
            df = db.execute_df(check_query, [stop_id])
            if df.iloc[0]['count'] == 0:
                raise HTTPException(status_code=404, detail=f"Stop {stop_id} not found or has no scheduled departures")
        return departures
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))