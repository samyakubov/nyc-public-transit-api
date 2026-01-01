"""
Input validation utilities for the transit API.
Provides validation functions for coordinates, IDs, time formats, and query parameters.
"""

import re
from typing import Optional, List, Any, Dict
from datetime import datetime, time
from pydantic import BaseModel, validator


class CoordinateValidationError(ValueError):
    """Raised when coordinate validation fails."""
    pass


class TimeValidationError(ValueError):
    """Raised when time format validation fails."""
    pass


class IDValidationError(ValueError):
    """Raised when ID format validation fails."""
    pass


def validate_latitude(lat: float) -> float:
    """
    Validate latitude is within valid range.
    
    Args:
        lat: Latitude in decimal degrees
    
    Returns:
        Validated latitude
    
    Raises:
        CoordinateValidationError: If latitude is out of range
    """
    if not isinstance(lat, (int, float)):
        raise CoordinateValidationError(f"Latitude must be a number, got {type(lat)}")
    
    if not -90 <= lat <= 90:
        raise CoordinateValidationError(f"Latitude must be between -90 and 90, got {lat}")
    
    return float(lat)


def validate_longitude(lon: float) -> float:
    """
    Validate longitude is within valid range.
    
    Args:
        lon: Longitude in decimal degrees
    
    Returns:
        Validated longitude
    
    Raises:
        CoordinateValidationError: If longitude is out of range
    """
    if not isinstance(lon, (int, float)):
        raise CoordinateValidationError(f"Longitude must be a number, got {type(lon)}")
    
    if not -180 <= lon <= 180:
        raise CoordinateValidationError(f"Longitude must be between -180 and 180, got {lon}")
    
    return float(lon)


def validate_radius(radius: float, max_radius: float = 50.0, unit: str = "miles") -> float:
    """
    Validate radius is within reasonable bounds.
    
    Args:
        radius: Radius value
        max_radius: Maximum allowed radius
        unit: Distance unit for error messages
    
    Returns:
        Validated radius
    
    Raises:
        ValueError: If radius is invalid
    """
    if not isinstance(radius, (int, float)):
        raise ValueError(f"Radius must be a number, got {type(radius)}")
    
    if radius <= 0:
        raise ValueError(f"Radius must be positive, got {radius}")
    
    if radius > max_radius:
        raise ValueError(f"Radius cannot exceed {max_radius} {unit}, got {radius}")
    
    return float(radius)


def validate_gtfs_id(gtfs_id: str, id_type: str = "ID") -> str:
    """
    Validate GTFS ID format (alphanumeric with underscores, hyphens, periods).
    
    Args:
        gtfs_id: The ID to validate
        id_type: Type of ID for error messages (e.g., "stop_id", "route_id")
    
    Returns:
        Validated ID
    
    Raises:
        IDValidationError: If ID format is invalid
    """
    if not isinstance(gtfs_id, str):
        raise IDValidationError(f"{id_type} must be a string, got {type(gtfs_id)}")
    
    if not gtfs_id.strip():
        raise IDValidationError(f"{id_type} cannot be empty")
    
    # GTFS IDs can contain letters, numbers, underscores, hyphens, and periods
    if not re.match(r'^[a-zA-Z0-9._-]+$', gtfs_id):
        raise IDValidationError(f"{id_type} contains invalid characters: {gtfs_id}")
    
    if len(gtfs_id) > 255:
        raise IDValidationError(f"{id_type} too long (max 255 characters): {gtfs_id}")
    
    return gtfs_id.strip()


def validate_time_format(time_str: str) -> str:
    """
    Validate time string is in HH:MM:SS format (GTFS format).
    
    Args:
        time_str: Time string to validate
    
    Returns:
        Validated time string
    
    Raises:
        TimeValidationError: If time format is invalid
    """
    if not isinstance(time_str, str):
        raise TimeValidationError(f"Time must be a string, got {type(time_str)}")
    
    # GTFS allows times > 24:00:00 for next-day service
    time_pattern = r'^([0-9]{1,2}):([0-5][0-9]):([0-5][0-9])$'
    match = re.match(time_pattern, time_str)
    
    if not match:
        raise TimeValidationError(f"Invalid time format. Expected HH:MM:SS, got: {time_str}")
    
    hours, minutes, seconds = map(int, match.groups())
    
    if minutes > 59 or seconds > 59:
        raise TimeValidationError(f"Invalid time values in: {time_str}")
    
    return time_str


def validate_date_format(date_str: str) -> str:
    """
    Validate date string is in YYYY-MM-DD format.
    
    Args:
        date_str: Date string to validate
    
    Returns:
        Validated date string
    
    Raises:
        ValueError: If date format is invalid
    """
    if not isinstance(date_str, str):
        raise ValueError(f"Date must be a string, got {type(date_str)}")
    
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return date_str
    except ValueError:
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got: {date_str}")


def validate_search_query(query: str, min_length: int = 1, max_length: int = 100) -> str:
    """
    Validate search query string.
    
    Args:
        query: Search query to validate
        min_length: Minimum query length
        max_length: Maximum query length
    
    Returns:
        Validated and sanitized query
    
    Raises:
        ValueError: If query is invalid
    """
    if not isinstance(query, str):
        raise ValueError(f"Search query must be a string, got {type(query)}")
    
    query = query.strip()
    
    if len(query) < min_length:
        raise ValueError(f"Search query too short (minimum {min_length} characters)")
    
    if len(query) > max_length:
        raise ValueError(f"Search query too long (maximum {max_length} characters)")
    
    # Remove potentially dangerous characters but keep spaces and common punctuation
    sanitized = re.sub(r'[<>"\';\\]', '', query)
    
    return sanitized


def validate_pagination_params(page: Optional[int] = None, per_page: Optional[int] = None) -> Dict[str, int]:
    """
    Validate and normalize pagination parameters.
    
    Args:
        page: Page number (1-based)
        per_page: Items per page
    
    Returns:
        Dictionary with validated page and per_page values
    
    Raises:
        ValueError: If parameters are invalid
    """
    # Set defaults
    validated_page = 1 if page is None else page
    validated_per_page = 20 if per_page is None else per_page
    
    # Validate page
    if not isinstance(validated_page, int) or validated_page < 1:
        raise ValueError(f"Page must be a positive integer, got {validated_page}")
    
    # Validate per_page
    if not isinstance(validated_per_page, int) or validated_per_page < 1:
        raise ValueError(f"Per page must be a positive integer, got {validated_per_page}")
    
    if validated_per_page > 100:
        raise ValueError(f"Per page cannot exceed 100, got {validated_per_page}")
    
    return {
        'page': validated_page,
        'per_page': validated_per_page
    }


def sanitize_sql_input(input_str: str) -> str:
    """
    Basic SQL injection prevention by removing dangerous characters.
    Note: This is a basic sanitizer. Parameterized queries are still preferred.
    
    Args:
        input_str: String to sanitize
    
    Returns:
        Sanitized string
    """
    if not isinstance(input_str, str):
        return str(input_str)
    
    # Remove common SQL injection patterns
    dangerous_patterns = [';', '--', '/*', '*/', 'xp_', 'sp_', 'DROP', 'DELETE', 'INSERT', 'UPDATE']
    
    sanitized = input_str
    for pattern in dangerous_patterns:
        sanitized = sanitized.replace(pattern, '')
    
    return sanitized.strip()


class ValidationResult(BaseModel):
    """Result of validation operation."""
    is_valid: bool
    errors: List[str] = []
    warnings: List[str] = []
    validated_data: Optional[Dict[str, Any]] = None


def validate_bulk_coordinates(coordinates: List[Dict[str, float]]) -> ValidationResult:
    """
    Validate a list of coordinate pairs.
    
    Args:
        coordinates: List of dictionaries with 'lat' and 'lon' keys
    
    Returns:
        ValidationResult with validation status and any errors
    """
    errors = []
    warnings = []
    validated_coords = []
    
    for i, coord in enumerate(coordinates):
        try:
            if not isinstance(coord, dict):
                errors.append(f"Coordinate {i}: must be a dictionary")
                continue
            
            if 'lat' not in coord or 'lon' not in coord:
                errors.append(f"Coordinate {i}: missing 'lat' or 'lon' key")
                continue
            
            lat = validate_latitude(coord['lat'])
            lon = validate_longitude(coord['lon'])
            
            validated_coords.append({'lat': lat, 'lon': lon})
            
        except (CoordinateValidationError, ValueError) as e:
            errors.append(f"Coordinate {i}: {str(e)}")
    
    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        validated_data={'coordinates': validated_coords} if len(errors) == 0 else None
    )