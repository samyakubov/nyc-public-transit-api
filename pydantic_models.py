from typing import List, Optional, Generic, TypeVar
from pydantic import BaseModel, Field, validator
import re

T = TypeVar('T')

class RouteFeature(BaseModel):
    type: str = "Feature"
    properties: dict
    geometry: dict

class GeoJSONResponse(BaseModel):
    type: str = "FeatureCollection"
    features: List[RouteFeature]

class Stop(BaseModel):
    """Basic stop information model."""
    stop_id: str = Field(..., description="Unique identifier for the stop")
    stop_name: str = Field(..., description="Name of the stop")
    stop_lat: float = Field(..., ge=-90, le=90, description="Latitude of the stop")
    stop_lon: float = Field(..., ge=-180, le=180, description="Longitude of the stop")
    location_type: Optional[int] = Field(0, description="Type of location (0=stop, 1=station)")
    wheelchair_boarding: Optional[int] = Field(0, description="Wheelchair accessibility (0=no info, 1=accessible, 2=not accessible)")
    platform_code: Optional[str] = Field(None, description="Platform identifier")
    stop_desc: Optional[str] = Field(None, description="Description of the stop")
    zone_id: Optional[str] = Field(None, description="Fare zone identifier")

    @validator('stop_id')
    def validate_stop_id(cls, v):
        if not v or not v.strip():
            raise ValueError('stop_id cannot be empty')
        return v.strip()

class StopWithDistance(Stop):
    """Stop model with distance information for nearby searches."""
    distance_miles: float = Field(..., description="Distance from search point in miles")

class StopDeparture(BaseModel):
    """Departure information for a stop."""
    trip_id: str = Field(..., description="Unique identifier for the trip")
    route_id: str = Field(..., description="Unique identifier for the route")
    route_short_name: str = Field(..., description="Short name of the route")
    route_long_name: str = Field(..., description="Long name of the route")
    headsign: Optional[str] = Field(None, description="Trip headsign")
    departure_time: str = Field(..., description="Departure time in HH:MM:SS format")

    @validator('departure_time')
    def validate_departure_time(cls, v):
        if not re.match(r'^\d{1,2}:\d{2}:\d{2}$', v):
            raise ValueError('departure_time must be in HH:MM:SS format')
        return v

class RouteBasic(BaseModel):
    """Basic route information model."""
    route_id: str = Field(..., description="Unique identifier for the route")
    route_short_name: str = Field(..., description="Short name of the route")
    route_long_name: str = Field(..., description="Long name of the route")
    route_color: str = Field("FFFFFF", description="Route color in hex format")
    route_text_color: str = Field("000000", description="Route text color in hex format")
    route_type: int = Field(..., description="Type of transportation (0=tram, 1=subway, 2=rail, 3=bus, etc.)")

    @validator('route_color', 'route_text_color')
    def validate_color(cls, v):
        if not re.match(r'^[0-9A-Fa-f]{6}$', v):
            raise ValueError('Color must be a 6-digit hex code')
        return v.upper()

class RouteDetail(RouteBasic):
    """Detailed route information with stops and service days."""
    stops: List[Stop] = Field(default_factory=list, description="List of stops served by this route")
    route_desc: Optional[str] = Field(None, description="Description of the route")

class StopWithRoutes(Stop):
    """Stop model with associated routes."""
    routes: List[RouteBasic] = Field(default_factory=list, description="Routes serving this stop")

class Trip(BaseModel):
    """Trip information model."""
    trip_id: str = Field(..., description="Unique identifier for the trip")
    route_id: str = Field(..., description="Route this trip belongs to")
    service_id: str = Field(..., description="Service calendar identifier")
    trip_headsign: Optional[str] = Field(None, description="Trip headsign")
    direction_id: Optional[int] = Field(None, ge=0, le=1, description="Direction of travel (0 or 1)")
    shape_id: Optional[str] = Field(None, description="Shape identifier for the trip path")

    @validator('trip_id', 'route_id', 'service_id')
    def validate_ids(cls, v):
        if not v or not v.strip():
            raise ValueError('ID fields cannot be empty')
        return v.strip()

class TripStop(BaseModel):
    """Stop information within a trip."""
    stop_id: str = Field(..., description="Stop identifier")
    stop_name: str = Field(..., description="Stop name")
    arrival_time: str = Field(..., description="Arrival time in HH:MM:SS format")
    departure_time: str = Field(..., description="Departure time in HH:MM:SS format")
    stop_sequence: int = Field(..., ge=1, description="Order of stop in the trip")

    @validator('arrival_time', 'departure_time')
    def validate_time_format(cls, v):
        if not re.match(r'^\d{1,2}:\d{2}:\d{2}$', v):
            raise ValueError('Time must be in HH:MM:SS format')
        return v

class TripWithStops(Trip):
    """Trip model with complete stop sequence."""
    stops: List[TripStop] = Field(default_factory=list, description="Ordered list of stops for this trip")


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response model."""
    items: List[T] = Field(..., description="List of items for this page")
    total: int = Field(..., ge=0, description="Total number of items")
    page: int = Field(..., ge=1, description="Current page number")
    per_page: int = Field(..., ge=1, description="Items per page")
    has_next: bool = Field(..., description="Whether there is a next page")
    has_prev: bool = Field(..., description="Whether there is a previous page")

class ExportRequest(BaseModel):
    """Request model for data exports."""
    format: str = Field(..., description="Export format (json, csv, geojson)")
    filters: Optional[dict] = Field(None, description="Optional filters to apply")
    
    @validator('format')
    def validate_format(cls, v):
        if v not in ['json', 'csv', 'geojson']:
            raise ValueError('format must be one of: json, csv, geojson')
        return v

class ErrorDetail(BaseModel):
    """Detailed error information."""
    field: Optional[str] = Field(None, description="Field that caused the error")
    value: Optional[str] = Field(None, description="Invalid value")
    constraint: Optional[str] = Field(None, description="Constraint that was violated")

class ErrorResponse(BaseModel):
    """Standardized error response model."""
    error: dict = Field(..., description="Error information")
    
    class Config:
        schema_extra = {
            "example": {
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "Invalid coordinates provided",
                    "details": {
                        "field": "latitude",
                        "value": "invalid_value",
                        "constraint": "must be between -90 and 90"
                    },
                    "request_id": "req_123456789"
                }
            }
        }