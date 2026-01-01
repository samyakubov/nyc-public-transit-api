"""
Resource limit handling utilities for the transit API.
Implements export size limits, request size limits, and resource usage validation.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from fastapi import Request, Query, HTTPException
from functools import wraps
import asyncio
from utils.error_handling import error_handler, ErrorCode, create_rate_limit_error


class ResourceLimits:
    """Resource limit configuration and validation."""
    
    # Export size limits by endpoint category
    EXPORT_LIMITS = {
        "stops": {
            "default": 1000,
            "search": 500,
            "nearby": 100,
            "bulk_export": 10000
        },
        "routes": {
            "default": 500,
            "search": 200,
            "bulk_export": 2000
        },
        "trips": {
            "default": 1000,
            "route_trips": 500,
            "bulk_export": 5000
        },
        "system": {
            "alerts": 100,
            "stats": 50
        }
    }
    
    # Request size limits (in bytes)
    REQUEST_SIZE_LIMITS = {
        "default": 1024 * 1024,      # 1MB
        "export": 5 * 1024 * 1024,   # 5MB
        "search": 512 * 1024,        # 512KB
        "bulk": 10 * 1024 * 1024     # 10MB
    }
    
    # Pagination limits
    PAGINATION_LIMITS = {
        "max_per_page": 1000,
        "default_per_page": 20,
        "max_offset": 100000
    }
    
    # Time window limits for queries
    TIME_WINDOW_LIMITS = {
        "max_hours": 168,  # 1 week
        "max_days": 30,    # 1 month
        "default_hours": 24
    }


def get_endpoint_category(path: str) -> str:
    """Determine resource category based on endpoint path."""
    if "/stops" in path:
        if "/search" in path:
            return "stops.search"
        elif "/nearby" in path:
            return "stops.nearby"
        elif "/export" in path or "format=" in path:
            return "stops.bulk_export"
        else:
            return "stops.default"
    elif "/routes" in path:
        if "/search" in path:
            return "routes.search"
        elif "/export" in path or "format=" in path:
            return "routes.bulk_export"
        else:
            return "routes.default"
    elif "/trips" in path:
        if "/export" in path or "format=" in path:
            return "trips.bulk_export"
        else:
            return "trips.default"
    elif "/system" in path:
        if "/alerts" in path:
            return "system.alerts"
        else:
            return "system.stats"
    else:
        return "default"


def get_export_limit(endpoint_category: str) -> int:
    """Get export size limit for endpoint category."""
    category_parts = endpoint_category.split(".")
    
    if len(category_parts) >= 2:
        resource_type = category_parts[0]
        sub_category = category_parts[1]
        
        if resource_type in ResourceLimits.EXPORT_LIMITS:
            limits = ResourceLimits.EXPORT_LIMITS[resource_type]
            return limits.get(sub_category, limits.get("default", 1000))
    
    return 1000  # Default limit


def get_request_size_limit(endpoint_category: str) -> int:
    """Get request size limit for endpoint category."""
    if "export" in endpoint_category or "bulk" in endpoint_category:
        return ResourceLimits.REQUEST_SIZE_LIMITS["export"]
    elif "search" in endpoint_category:
        return ResourceLimits.REQUEST_SIZE_LIMITS["search"]
    else:
        return ResourceLimits.REQUEST_SIZE_LIMITS["default"]


def validate_export_size(
    requested_size: int,
    endpoint_category: str,
    request_id: Optional[str] = None
) -> None:
    """
    Validate export size against limits.
    
    Args:
        requested_size: Number of items requested
        endpoint_category: Endpoint category for limit lookup
        request_id: Optional request ID for error tracking
    
    Raises:
        HTTPException: If export size exceeds limits
    """
    limit = get_export_limit(endpoint_category)
    
    if requested_size > limit:
        error_handler.handle_rate_limit_error(
            limit_type="export",
            current_usage=requested_size,
            limit=limit,
            request_id=request_id
        )


def validate_pagination_params(
    page: Optional[int] = None,
    per_page: Optional[int] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    request_id: Optional[str] = None
) -> Dict[str, int]:
    """
    Validate and normalize pagination parameters.
    
    Args:
        page: Page number (1-based)
        per_page: Items per page
        offset: Offset for cursor-based pagination
        limit: Limit for simple pagination
        request_id: Optional request ID for error tracking
    
    Returns:
        Dictionary with validated pagination parameters
    
    Raises:
        HTTPException: If parameters are invalid
    """
    # Normalize parameters
    if limit is not None and per_page is None:
        per_page = limit
    
    validated_page = 1 if page is None else page
    validated_per_page = ResourceLimits.PAGINATION_LIMITS["default_per_page"] if per_page is None else per_page
    validated_offset = 0 if offset is None else offset
    
    # Validate page
    if validated_page < 1:
        error_handler.handle_validation_error(
            field="page",
            value=validated_page,
            constraint="must be a positive integer (1 or greater)",
            request_id=request_id
        )
    
    # Validate per_page/limit
    if validated_per_page < 1:
        error_handler.handle_validation_error(
            field="per_page" if per_page is not None else "limit",
            value=validated_per_page,
            constraint="must be a positive integer (1 or greater)",
            request_id=request_id
        )
    
    if validated_per_page > ResourceLimits.PAGINATION_LIMITS["max_per_page"]:
        error_handler.handle_validation_error(
            field="per_page" if per_page is not None else "limit",
            value=validated_per_page,
            constraint=f"cannot exceed {ResourceLimits.PAGINATION_LIMITS['max_per_page']}",
            request_id=request_id
        )
    
    # Validate offset
    if validated_offset < 0:
        error_handler.handle_validation_error(
            field="offset",
            value=validated_offset,
            constraint="must be non-negative",
            request_id=request_id
        )
    
    if validated_offset > ResourceLimits.PAGINATION_LIMITS["max_offset"]:
        error_handler.handle_validation_error(
            field="offset",
            value=validated_offset,
            constraint=f"cannot exceed {ResourceLimits.PAGINATION_LIMITS['max_offset']}",
            request_id=request_id
        )
    
    return {
        "page": validated_page,
        "per_page": validated_per_page,
        "offset": validated_offset,
        "limit": validated_per_page  # For backward compatibility
    }


def validate_time_window(
    time_window_hours: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    request_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Validate time window parameters.
    
    Args:
        time_window_hours: Time window in hours from now
        start_time: Start time in ISO format
        end_time: End time in ISO format
        request_id: Optional request ID for error tracking
    
    Returns:
        Dictionary with validated time parameters
    
    Raises:
        HTTPException: If time parameters are invalid
    """
    if time_window_hours is not None:
        if time_window_hours < 1:
            error_handler.handle_validation_error(
                field="time_window_hours",
                value=time_window_hours,
                constraint="must be at least 1 hour",
                request_id=request_id
            )
        
        if time_window_hours > ResourceLimits.TIME_WINDOW_LIMITS["max_hours"]:
            error_handler.handle_validation_error(
                field="time_window_hours",
                value=time_window_hours,
                constraint=f"cannot exceed {ResourceLimits.TIME_WINDOW_LIMITS['max_hours']} hours",
                request_id=request_id
            )
    
    if start_time and end_time:
        try:
            start_dt = datetime.fromisoformat(start_time)
            end_dt = datetime.fromisoformat(end_time)
            
            if start_dt >= end_dt:
                error_handler.handle_validation_error(
                    field="time_range",
                    value=f"{start_time} to {end_time}",
                    constraint="start_time must be before end_time",
                    request_id=request_id
                )
            
            # Check if time range is too large
            time_diff = end_dt - start_dt
            if time_diff.total_seconds() > ResourceLimits.TIME_WINDOW_LIMITS["max_hours"] * 3600:
                error_handler.handle_validation_error(
                    field="time_range",
                    value=f"{start_time} to {end_time}",
                    constraint=f"time range cannot exceed {ResourceLimits.TIME_WINDOW_LIMITS['max_hours']} hours",
                    request_id=request_id
                )
                
        except ValueError as e:
            error_handler.handle_validation_error(
                field="time_format",
                value=f"start_time={start_time}, end_time={end_time}",
                constraint="times must be in ISO format (YYYY-MM-DDTHH:MM:SS)",
                request_id=request_id
            )
    
    return {
        "time_window_hours": time_window_hours or ResourceLimits.TIME_WINDOW_LIMITS["default_hours"],
        "start_time": start_time,
        "end_time": end_time
    }


def validate_search_parameters(
    query: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    request_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Validate search parameters.
    
    Args:
        query: Search query string
        filters: Additional search filters
        request_id: Optional request ID for error tracking
    
    Returns:
        Dictionary with validated search parameters
    
    Raises:
        HTTPException: If search parameters are invalid
    """
    validated_params = {}
    
    if query is not None:
        from utils.validation import validate_search_query
        try:
            validated_params["query"] = validate_search_query(query, min_length=1, max_length=200)
        except ValueError as e:
            error_handler.handle_validation_error(
                field="query",
                value=query,
                constraint=str(e),
                request_id=request_id
            )
    
    if filters is not None:
        # Validate filter complexity
        if len(filters) > 10:
            error_handler.handle_validation_error(
                field="filters",
                value=f"{len(filters)} filters",
                constraint="cannot have more than 10 filters",
                request_id=request_id
            )
        
        # Validate individual filter values
        for key, value in filters.items():
            if isinstance(value, str) and len(value) > 100:
                error_handler.handle_validation_error(
                    field=f"filters.{key}",
                    value=value,
                    constraint="filter value cannot exceed 100 characters",
                    request_id=request_id
                )
        
        validated_params["filters"] = filters
    
    return validated_params


class ResourceLimitValidator:
    """Decorator class for resource limit validation."""
    
    @staticmethod
    def validate_export_limits(max_size: Optional[int] = None):
        """Decorator to validate export size limits."""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Extract request from args/kwargs
                request = None
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break
                
                if not request:
                    request = kwargs.get('request')
                
                if request:
                    endpoint_category = get_endpoint_category(request.url.path)
                    limit = max_size or get_export_limit(endpoint_category)
                    
                    # Check various size parameters
                    size_params = ['limit', 'per_page', 'count', 'size']
                    for param in size_params:
                        if param in kwargs and kwargs[param]:
                            validate_export_size(
                                kwargs[param],
                                endpoint_category,
                                request.headers.get("X-Request-ID")
                            )
                
                return await func(*args, **kwargs)
            return wrapper
        return decorator
    
    @staticmethod
    def validate_pagination():
        """Decorator to validate pagination parameters."""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Extract request from args/kwargs
                request = None
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break
                
                if not request:
                    request = kwargs.get('request')
                
                request_id = request.headers.get("X-Request-ID") if request else None
                
                # Validate pagination parameters
                pagination_params = validate_pagination_params(
                    page=kwargs.get('page'),
                    per_page=kwargs.get('per_page'),
                    offset=kwargs.get('offset'),
                    limit=kwargs.get('limit'),
                    request_id=request_id
                )
                
                # Update kwargs with validated parameters
                kwargs.update(pagination_params)
                
                return await func(*args, **kwargs)
            return wrapper
        return decorator
    
    @staticmethod
    def validate_time_windows():
        """Decorator to validate time window parameters."""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Extract request from args/kwargs
                request = None
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break
                
                if not request:
                    request = kwargs.get('request')
                
                request_id = request.headers.get("X-Request-ID") if request else None
                
                # Validate time window parameters
                time_params = validate_time_window(
                    time_window_hours=kwargs.get('time_window_hours'),
                    start_time=kwargs.get('start_time'),
                    end_time=kwargs.get('end_time'),
                    request_id=request_id
                )
                
                # Update kwargs with validated parameters
                kwargs.update(time_params)
                
                return await func(*args, **kwargs)
            return wrapper
        return decorator


def create_resource_limit_guidance(
    limit_type: str,
    current_value: int,
    limit_value: int
) -> str:
    """
    Create helpful guidance message for resource limit errors.
    
    Args:
        limit_type: Type of limit exceeded
        current_value: Current requested value
        limit_value: Maximum allowed value
    
    Returns:
        Helpful guidance message
    """
    guidance_messages = {
        "export": f"Reduce the number of items requested from {current_value} to {limit_value} or less. "
                 f"Consider using pagination to retrieve data in smaller chunks.",
        
        "pagination": f"Use pagination with per_page <= {limit_value}. "
                     f"For large datasets, iterate through multiple pages.",
        
        "time_window": f"Reduce the time window from {current_value} hours to {limit_value} hours or less. "
                      f"For longer periods, make multiple requests with smaller time ranges.",
        
        "search_complexity": f"Simplify your search by reducing the number of filters from {current_value} to {limit_value} or less.",
        
        "request_size": f"Reduce request size from {current_value} bytes to {limit_value} bytes or less. "
                       f"Consider breaking large requests into smaller ones."
    }
    
    return guidance_messages.get(limit_type, 
        f"Reduce the requested value from {current_value} to {limit_value} or less.")


# Convenience functions for common validations
async def validate_export_request(
    request: Request,
    limit: Optional[int] = None,
    per_page: Optional[int] = None,
    format_type: Optional[str] = None
) -> Dict[str, Any]:
    """Validate export request parameters."""
    endpoint_category = get_endpoint_category(request.url.path)
    request_id = request.headers.get("X-Request-ID")
    
    # Determine export size
    export_size = limit or per_page or 100
    
    # Validate export size
    validate_export_size(export_size, endpoint_category, request_id)
    
    # Validate format if provided
    if format_type and format_type not in ['json', 'csv', 'geojson']:
        error_handler.handle_validation_error(
            field="format",
            value=format_type,
            constraint="must be one of: json, csv, geojson",
            request_id=request_id
        )
    
    return {
        "export_size": export_size,
        "format": format_type or "json",
        "endpoint_category": endpoint_category
    }


async def validate_bulk_request(
    request: Request,
    item_count: int,
    operation_type: str = "export"
) -> Dict[str, Any]:
    """Validate bulk operation request."""
    endpoint_category = get_endpoint_category(request.url.path)
    request_id = request.headers.get("X-Request-ID")
    
    # Get appropriate limit for bulk operations
    if operation_type == "export":
        limit = get_export_limit(f"{endpoint_category}.bulk_export")
    else:
        limit = get_export_limit(endpoint_category)
    
    if item_count > limit:
        error_handler.handle_rate_limit_error(
            limit_type="bulk_operation",
            current_usage=item_count,
            limit=limit,
            request_id=request_id
        )
    
    return {
        "validated_count": item_count,
        "limit": limit,
        "operation_type": operation_type
    }