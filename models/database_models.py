"""
Database model definitions for the transit API.
These models represent the structure of data as stored in the database.
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class DatabaseStop:
    """Database representation of a transit stop."""
    stop_id: str
    stop_name: str
    stop_desc: Optional[str]
    stop_lat: float
    stop_lon: float
    zone_id: Optional[str]
    stop_url: Optional[str]
    location_type: Optional[int]
    parent_station: Optional[str]
    stop_timezone: Optional[str]
    wheelchair_boarding: Optional[int]
    level_id: Optional[str]
    platform_code: Optional[str]


@dataclass
class DatabaseRoute:
    """Database representation of a transit route."""
    route_id: str
    agency_id: Optional[str]
    route_short_name: str
    route_long_name: str
    route_desc: Optional[str]
    route_type: int
    route_url: Optional[str]
    route_color: Optional[str]
    route_text_color: Optional[str]
    route_sort_order: Optional[int]


@dataclass
class DatabaseTrip:
    """Database representation of a transit trip."""
    route_id: str
    service_id: str
    trip_id: str
    trip_headsign: Optional[str]
    trip_short_name: Optional[str]
    direction_id: Optional[int]
    block_id: Optional[str]
    shape_id: Optional[str]
    wheelchair_accessible: Optional[int]
    bikes_allowed: Optional[int]


@dataclass
class DatabaseStopTime:
    """Database representation of a stop time."""
    trip_id: str
    arrival_time: str
    departure_time: str
    stop_id: str
    stop_sequence: int
    stop_headsign: Optional[str]
    pickup_type: Optional[int]
    drop_off_type: Optional[int]
    shape_dist_traveled: Optional[float]
    timepoint: Optional[int]


@dataclass
class DatabaseCalendar:
    """Database representation of service calendar."""
    service_id: str
    monday: int
    tuesday: int
    wednesday: int
    thursday: int
    friday: int
    saturday: int
    sunday: int
    start_date: str
    end_date: str


@dataclass
class DatabaseCalendarDate:
    """Database representation of calendar exceptions."""
    service_id: str
    date: str
    exception_type: int


@dataclass
class DatabaseShape:
    """Database representation of route shapes."""
    shape_id: str
    shape_pt_lat: float
    shape_pt_lon: float
    shape_pt_sequence: int
    shape_dist_traveled: Optional[float]


@dataclass
class DatabaseAgency:
    """Database representation of transit agency."""
    agency_id: Optional[str]
    agency_name: str
    agency_url: str
    agency_timezone: str
    agency_lang: Optional[str]
    agency_phone: Optional[str]
    agency_fare_url: Optional[str]
    agency_email: Optional[str]