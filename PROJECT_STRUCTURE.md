# Enhanced Project Structure

This document describes the enhanced project structure for the Transit API Expansion.

## Directory Structure

```
├── endpoints/                    # FastAPI routers for different endpoint categories
│   ├── routes.py                # Existing routes endpoints
│   ├── stops.py                 # Stop-related endpoints
│   ├── trips.py                 # Trip-related endpoints  
│   ├── journey.py               # Journey planning endpoints
│   └── system.py                # System status endpoints
├── endpoint_handlers/            # Business logic handlers
│   ├── get_nearby_routes.py     # Existing route handlers
│   ├── stop_handlers.py         # Stop operation handlers
│   ├── trip_handlers.py         # Trip operation handlers
│   ├── journey_handlers.py      # Journey planning handlers
│   └── system_handlers.py       # System status handlers
├── utils/                       # Utility modules
│   ├── geospatial.py           # Geospatial calculations and validation
│   ├── validation.py           # Input validation utilities
│   ├── pagination.py           # Pagination helpers
│   └── caching.py              # Caching utilities with TTL support
├── models/                      # Data models
│   ├── pydantic_models.py      # Existing API models (to be enhanced)
│   └── database_models.py      # Database representation models
└── database_connector.py       # Enhanced with spatial extension support
```

## Key Features Added

### 1. Geospatial Utilities (`utils/geospatial.py`)
- Haversine distance calculations
- Coordinate validation
- Bounding box calculations
- Point-in-polygon testing
- Unit conversions (miles, kilometers, meters)

### 2. Validation Utilities (`utils/validation.py`)
- Coordinate range validation
- GTFS ID format validation
- Time format validation (HH:MM:SS)
- Search query sanitization
- Pagination parameter validation

### 3. Pagination Utilities (`utils/pagination.py`)
- Offset-based pagination for smaller datasets
- Cursor-based pagination for large datasets
- Pagination metadata generation
- Navigation URL generation

### 4. Caching Utilities (`utils/caching.py`)
- In-memory cache with TTL support
- Function result caching decorator
- Cache statistics and monitoring
- HTTP cache header generation

### 5. Database Enhancements
- DuckDB spatial extension automatically installed and loaded
- Spatial functions available for geospatial queries
- Enhanced error handling

## Dependencies Added

- `hypothesis`: Property-based testing framework
- DuckDB spatial extension (auto-installed)

## Usage Examples

### Geospatial Operations
```python
from utils.geospatial import haversine_distance, validate_coordinates

# Calculate distance between two points
distance = haversine_distance(40.7128, -74.0060, 40.7589, -73.9851, unit="miles")

# Validate coordinates
is_valid = validate_coordinates(40.7128, -74.0060)
```

### Input Validation
```python
from utils.validation import validate_latitude, validate_gtfs_id

# Validate coordinates
lat = validate_latitude(40.7128)
stop_id = validate_gtfs_id("stop_123", "stop_id")
```

### Pagination
```python
from utils.pagination import create_paginated_response

# Create paginated response
response = create_paginated_response(items, total=100, page=1, per_page=20)
```

### Caching
```python
from utils.caching import cached

@cached(ttl=300)  # Cache for 5 minutes
def expensive_operation(param):
    return perform_calculation(param)
```

### Spatial Database Queries
```python
from database_connector import DatabaseConnector

db = DatabaseConnector("transit.duckdb")
conn = db.connect()

# Use spatial functions
result = conn.execute("""
    SELECT stop_id, stop_name,
           ST_Distance(ST_Point(stop_lon, stop_lat), ST_Point(?, ?)) * 69 as distance_miles
    FROM stops 
    WHERE ST_DWithin(ST_Point(stop_lon, stop_lat), ST_Point(?, ?), ? / 69.0)
    ORDER BY distance_miles
""", [lon, lat, lon, lat, radius_miles]).fetchall()
```

## Next Steps

The enhanced project structure is now ready for implementing the specific endpoints and handlers in subsequent tasks. Each utility module provides comprehensive functionality for the requirements specified in the design document.