"""
Geospatial utility functions for transit API operations.
Provides distance calculations, coordinate validation, and spatial query helpers.
"""

import math
from typing import Tuple, List, Optional


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float, unit: str = "miles") -> float:
    """
    Calculate the great circle distance between two points on Earth using the Haversine formula.
    
    Args:
        lat1, lon1: Latitude and longitude of first point in decimal degrees
        lat2, lon2: Latitude and longitude of second point in decimal degrees
        unit: Distance unit - "miles", "kilometers", or "meters"
    
    Returns:
        Distance between the two points in the specified unit
    """
    
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    
    earth_radius = {
        "miles": 3959,
        "kilometers": 6371,
        "meters": 6371000
    }
    
    if unit not in earth_radius:
        raise ValueError(f"Unsupported unit: {unit}. Use 'miles', 'kilometers', or 'meters'")
    
    return c * earth_radius[unit]


def validate_coordinates(lat: float, lon: float) -> bool:
    """
    Validate that latitude and longitude are within valid ranges.
    
    Args:
        lat: Latitude in decimal degrees
        lon: Longitude in decimal degrees
    
    Returns:
        True if coordinates are valid, False otherwise
    """
    return -90 <= lat <= 90 and -180 <= lon <= 180


def calculate_bounding_box(lat: float, lon: float, radius_miles: float) -> Tuple[float, float, float, float]:
    """
    Calculate a bounding box around a point for efficient spatial queries.
    
    Args:
        lat: Center latitude in decimal degrees
        lon: Center longitude in decimal degrees
        radius_miles: Radius in miles
    
    Returns:
        Tuple of (min_lat, min_lon, max_lat, max_lon)
    """
    
    lat_degrees_per_mile = 1 / 69.0
    lon_degrees_per_mile = 1 / (69.0 * math.cos(math.radians(lat)))
    
    lat_offset = radius_miles * lat_degrees_per_mile
    lon_offset = radius_miles * lon_degrees_per_mile
    
    return (
        lat - lat_offset,  
        lon - lon_offset,  
        lat + lat_offset,  
        lon + lon_offset   
    )


def point_in_polygon(lat: float, lon: float, polygon_coords: List[Tuple[float, float]]) -> bool:
    """
    Check if a point is inside a polygon using the ray casting algorithm.
    
    Args:
        lat: Point latitude
        lon: Point longitude
        polygon_coords: List of (lat, lon) tuples defining the polygon vertices
    
    Returns:
        True if point is inside polygon, False otherwise
    """
    x, y = lon, lat
    n = len(polygon_coords)
    inside = False
    
    p1x, p1y = polygon_coords[0]
    for i in range(1, n + 1):
        p2x, p2y = polygon_coords[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    
    return inside


def degrees_to_miles(degrees: float, latitude: float) -> float:
    """
    Convert degrees to miles at a given latitude.
    
    Args:
        degrees: Distance in degrees
        latitude: Latitude for longitude correction
    
    Returns:
        Distance in miles
    """
    lat_miles = degrees * 69.0
    lon_miles = degrees * 69.0 * math.cos(math.radians(latitude))
    return math.sqrt(lat_miles**2 + lon_miles**2)


def miles_to_degrees(miles: float, latitude: float) -> float:
    """
    Convert miles to degrees at a given latitude.
    
    Args:
        miles: Distance in miles
        latitude: Latitude for longitude correction
    
    Returns:
        Distance in degrees
    """
    return miles / (69.0 * math.cos(math.radians(latitude)))