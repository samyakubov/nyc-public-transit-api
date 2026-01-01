"""
Comprehensive error handling utilities for the transit API.
Provides standardized error responses, HTTP status code handling, and error categorization.
"""

import uuid
from typing import Optional, Dict, Any, List, Union
from datetime import datetime
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
import logging

# Configure logging
logger = logging.getLogger(__name__)


class ErrorCode:
    """Standard error codes for the transit API."""
    
    # Validation Errors (400)
    VALIDATION_ERROR = "VALIDATION_ERROR"
    INVALID_COORDINATES = "INVALID_COORDINATES"
    INVALID_TIME_FORMAT = "INVALID_TIME_FORMAT"
    INVALID_DATE_FORMAT = "INVALID_DATE_FORMAT"
    INVALID_ID_FORMAT = "INVALID_ID_FORMAT"
    MISSING_PARAMETER = "MISSING_PARAMETER"
    PARAMETER_OUT_OF_RANGE = "PARAMETER_OUT_OF_RANGE"
    INVALID_SEARCH_QUERY = "INVALID_SEARCH_QUERY"
    INVALID_EXPORT_FORMAT = "INVALID_EXPORT_FORMAT"
    
    # Resource Not Found Errors (404)
    STOP_NOT_FOUND = "STOP_NOT_FOUND"
    ROUTE_NOT_FOUND = "ROUTE_NOT_FOUND"
    TRIP_NOT_FOUND = "TRIP_NOT_FOUND"
    SHAPE_NOT_FOUND = "SHAPE_NOT_FOUND"
    NO_RESULTS_FOUND = "NO_RESULTS_FOUND"
    
    # Rate Limiting Errors (429)
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    EXPORT_LIMIT_EXCEEDED = "EXPORT_LIMIT_EXCEEDED"
    REQUEST_SIZE_LIMIT_EXCEEDED = "REQUEST_SIZE_LIMIT_EXCEEDED"
    
    # Server Errors (500)
    DATABASE_ERROR = "DATABASE_ERROR"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    CACHE_ERROR = "CACHE_ERROR"
    EXTERNAL_SERVICE_ERROR = "EXTERNAL_SERVICE_ERROR"
    
    # Service Unavailable (503)
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    MAINTENANCE_MODE = "MAINTENANCE_MODE"


class ErrorDetail:
    """Detailed error information structure."""
    
    def __init__(
        self,
        field: Optional[str] = None,
        value: Optional[str] = None,
        constraint: Optional[str] = None,
        location: Optional[str] = None,
        additional_info: Optional[Dict[str, Any]] = None
    ):
        self.field = field
        self.value = value
        self.constraint = constraint
        self.location = location
        self.additional_info = additional_info or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {}
        if self.field:
            result["field"] = self.field
        if self.value is not None:
            result["value"] = str(self.value)
        if self.constraint:
            result["constraint"] = self.constraint
        if self.location:
            result["location"] = self.location
        if self.additional_info:
            result.update(self.additional_info)
        return result


class StandardizedError:
    """Standardized error response structure."""
    
    def __init__(
        self,
        code: str,
        message: str,
        details: Optional[Union[ErrorDetail, Dict[str, Any], List[ErrorDetail]]] = None,
        request_id: Optional[str] = None,
        timestamp: Optional[str] = None,
        help_url: Optional[str] = None
    ):
        self.code = code
        self.message = message
        self.details = details
        self.request_id = request_id or str(uuid.uuid4())
        self.timestamp = timestamp or datetime.now().isoformat()
        self.help_url = help_url
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON response."""
        error_dict = {
            "code": self.code,
            "message": self.message,
            "request_id": self.request_id,
            "timestamp": self.timestamp
        }
        
        if self.details:
            if isinstance(self.details, ErrorDetail):
                error_dict["details"] = self.details.to_dict()
            elif isinstance(self.details, list):
                error_dict["details"] = [
                    detail.to_dict() if isinstance(detail, ErrorDetail) else detail
                    for detail in self.details
                ]
            else:
                error_dict["details"] = self.details
        
        if self.help_url:
            error_dict["help_url"] = self.help_url
        
        return {"error": error_dict}


def create_validation_error(
    message: str,
    field: Optional[str] = None,
    value: Optional[str] = None,
    constraint: Optional[str] = None,
    request_id: Optional[str] = None
) -> StandardizedError:
    """Create a standardized validation error."""
    details = ErrorDetail(field=field, value=value, constraint=constraint)
    return StandardizedError(
        code=ErrorCode.VALIDATION_ERROR,
        message=message,
        details=details,
        request_id=request_id,
        help_url="https://api-docs.transit.example.com/errors#validation"
    )


def create_not_found_error(
    resource_type: str,
    resource_id: str,
    request_id: Optional[str] = None
) -> StandardizedError:
    """Create a standardized not found error."""
    code_map = {
        "stop": ErrorCode.STOP_NOT_FOUND,
        "route": ErrorCode.ROUTE_NOT_FOUND,
        "trip": ErrorCode.TRIP_NOT_FOUND,
        "shape": ErrorCode.SHAPE_NOT_FOUND
    }
    
    code = code_map.get(resource_type.lower(), ErrorCode.NO_RESULTS_FOUND)
    message = f"{resource_type.title()} with ID '{resource_id}' not found"
    
    details = ErrorDetail(
        field=f"{resource_type.lower()}_id",
        value=resource_id,
        constraint=f"must be a valid {resource_type.lower()} identifier"
    )
    
    return StandardizedError(
        code=code,
        message=message,
        details=details,
        request_id=request_id,
        help_url="https://api-docs.transit.example.com/errors#not-found"
    )


def create_rate_limit_error(
    limit_type: str,
    current_usage: int,
    limit: int,
    reset_time: Optional[str] = None,
    request_id: Optional[str] = None
) -> StandardizedError:
    """Create a standardized rate limit error."""
    if limit_type == "export":
        code = ErrorCode.EXPORT_LIMIT_EXCEEDED
        message = f"Export size limit exceeded. Requested {current_usage} items, limit is {limit}"
    elif limit_type == "request_size":
        code = ErrorCode.REQUEST_SIZE_LIMIT_EXCEEDED
        message = f"Request size limit exceeded. Request size {current_usage} bytes, limit is {limit} bytes"
    else:
        code = ErrorCode.RATE_LIMIT_EXCEEDED
        message = f"Rate limit exceeded. Current usage: {current_usage}, limit: {limit}"
    
    details = ErrorDetail(
        additional_info={
            "limit_type": limit_type,
            "current_usage": current_usage,
            "limit": limit,
            "reset_time": reset_time,
            "guidance": "Reduce request frequency or use pagination for large datasets"
        }
    )
    
    return StandardizedError(
        code=code,
        message=message,
        details=details,
        request_id=request_id,
        help_url="https://api-docs.transit.example.com/errors#rate-limits"
    )


def create_database_error(
    operation: str,
    request_id: Optional[str] = None,
    include_debug_info: bool = False,
    debug_info: Optional[str] = None
) -> StandardizedError:
    """Create a standardized database error."""
    message = f"Database error occurred during {operation}"
    
    details = ErrorDetail(
        additional_info={
            "operation": operation,
            "guidance": "Please try again later. If the problem persists, contact support."
        }
    )
    
    # Only include debug info in development/testing environments
    if include_debug_info and debug_info:
        details.additional_info["debug_info"] = debug_info
    
    return StandardizedError(
        code=ErrorCode.DATABASE_ERROR,
        message=message,
        details=details,
        request_id=request_id,
        help_url="https://api-docs.transit.example.com/errors#server-errors"
    )


def create_system_error(
    component: str,
    request_id: Optional[str] = None,
    include_debug_info: bool = False,
    debug_info: Optional[str] = None
) -> StandardizedError:
    """Create a standardized system error."""
    message = f"System error in {component} component"
    
    details = ErrorDetail(
        additional_info={
            "component": component,
            "guidance": "Please try again later. If the problem persists, contact support."
        }
    )
    
    # Only include debug info in development/testing environments
    if include_debug_info and debug_info:
        details.additional_info["debug_info"] = debug_info
    
    return StandardizedError(
        code=ErrorCode.SYSTEM_ERROR,
        message=message,
        details=details,
        request_id=request_id,
        help_url="https://api-docs.transit.example.com/errors#server-errors"
    )


def handle_pydantic_validation_error(
    validation_error: ValidationError,
    request_id: Optional[str] = None
) -> StandardizedError:
    """Convert Pydantic validation errors to standardized format."""
    errors = validation_error.errors()
    
    if len(errors) == 1:
        # Single validation error
        error = errors[0]
        field = ".".join(str(loc) for loc in error["loc"])
        
        return create_validation_error(
            message=error["msg"],
            field=field,
            value=str(error.get("input", "")),
            constraint=error["type"],
            request_id=request_id
        )
    else:
        # Multiple validation errors
        error_details = []
        for error in errors:
            field = ".".join(str(loc) for loc in error["loc"])
            error_details.append(ErrorDetail(
                field=field,
                value=str(error.get("input", "")),
                constraint=error["msg"]
            ))
        
        return StandardizedError(
            code=ErrorCode.VALIDATION_ERROR,
            message=f"Multiple validation errors ({len(errors)} errors)",
            details=error_details,
            request_id=request_id,
            help_url="https://api-docs.transit.example.com/errors#validation"
        )


def create_http_exception(
    error: StandardizedError,
    status_code: int,
    headers: Optional[Dict[str, str]] = None
) -> HTTPException:
    """Create an HTTPException with standardized error format."""
    return HTTPException(
        status_code=status_code,
        detail=error.to_dict(),
        headers=headers
    )


def get_status_code_for_error_code(error_code: str) -> int:
    """Get appropriate HTTP status code for error code."""
    status_code_map = {
        # 400 Bad Request
        ErrorCode.VALIDATION_ERROR: 400,
        ErrorCode.INVALID_COORDINATES: 400,
        ErrorCode.INVALID_TIME_FORMAT: 400,
        ErrorCode.INVALID_DATE_FORMAT: 400,
        ErrorCode.INVALID_ID_FORMAT: 400,
        ErrorCode.MISSING_PARAMETER: 400,
        ErrorCode.PARAMETER_OUT_OF_RANGE: 400,
        ErrorCode.INVALID_SEARCH_QUERY: 400,
        ErrorCode.INVALID_EXPORT_FORMAT: 400,
        
        # 404 Not Found
        ErrorCode.STOP_NOT_FOUND: 404,
        ErrorCode.ROUTE_NOT_FOUND: 404,
        ErrorCode.TRIP_NOT_FOUND: 404,
        ErrorCode.SHAPE_NOT_FOUND: 404,
        ErrorCode.NO_RESULTS_FOUND: 404,
        
        # 429 Too Many Requests
        ErrorCode.RATE_LIMIT_EXCEEDED: 429,
        ErrorCode.EXPORT_LIMIT_EXCEEDED: 429,
        ErrorCode.REQUEST_SIZE_LIMIT_EXCEEDED: 429,
        
        # 500 Internal Server Error
        ErrorCode.DATABASE_ERROR: 500,
        ErrorCode.SYSTEM_ERROR: 500,
        ErrorCode.CACHE_ERROR: 500,
        ErrorCode.EXTERNAL_SERVICE_ERROR: 500,
        
        # 503 Service Unavailable
        ErrorCode.SERVICE_UNAVAILABLE: 503,
        ErrorCode.MAINTENANCE_MODE: 503,
    }
    
    return status_code_map.get(error_code, 500)


def raise_standardized_error(error: StandardizedError, headers: Optional[Dict[str, str]] = None):
    """Raise an HTTPException with standardized error format."""
    status_code = get_status_code_for_error_code(error.code)
    raise create_http_exception(error, status_code, headers)


class ErrorHandler:
    """Centralized error handler for the transit API."""
    
    def __init__(self, include_debug_info: bool = False):
        self.include_debug_info = include_debug_info
    
    def handle_validation_error(
        self,
        field: str,
        value: Any,
        constraint: str,
        request_id: Optional[str] = None
    ):
        """Handle validation errors with standardized response."""
        error = create_validation_error(
            message=f"Invalid value for {field}",
            field=field,
            value=str(value),
            constraint=constraint,
            request_id=request_id
        )
        raise_standardized_error(error)
    
    def handle_not_found(
        self,
        resource_type: str,
        resource_id: str,
        request_id: Optional[str] = None
    ):
        """Handle not found errors with standardized response."""
        error = create_not_found_error(resource_type, resource_id, request_id)
        raise_standardized_error(error)
    
    def handle_database_error(
        self,
        operation: str,
        original_error: Exception,
        request_id: Optional[str] = None
    ):
        """Handle database errors with standardized response."""
        # Log the original error for debugging
        logger.error(f"Database error in {operation}: {str(original_error)}", exc_info=True)
        
        error = create_database_error(
            operation=operation,
            request_id=request_id,
            include_debug_info=self.include_debug_info,
            debug_info=str(original_error) if self.include_debug_info else None
        )
        raise_standardized_error(error)
    
    def handle_system_error(
        self,
        component: str,
        original_error: Exception,
        request_id: Optional[str] = None
    ):
        """Handle system errors with standardized response."""
        # Log the original error for debugging
        logger.error(f"System error in {component}: {str(original_error)}", exc_info=True)
        
        error = create_system_error(
            component=component,
            request_id=request_id,
            include_debug_info=self.include_debug_info,
            debug_info=str(original_error) if self.include_debug_info else None
        )
        raise_standardized_error(error)
    
    def handle_rate_limit_error(
        self,
        limit_type: str,
        current_usage: int,
        limit: int,
        reset_time: Optional[str] = None,
        request_id: Optional[str] = None
    ):
        """Handle rate limit errors with standardized response."""
        error = create_rate_limit_error(
            limit_type=limit_type,
            current_usage=current_usage,
            limit=limit,
            reset_time=reset_time,
            request_id=request_id
        )
        
        # Add rate limit headers
        headers = {
            "X-RateLimit-Limit": str(limit),
            "X-RateLimit-Remaining": str(max(0, limit - current_usage)),
        }
        if reset_time:
            headers["X-RateLimit-Reset"] = reset_time
        
        raise_standardized_error(error, headers)


# Global error handler instance
error_handler = ErrorHandler()


def get_request_id(request: Request) -> str:
    """Extract or generate request ID for error tracking."""
    # Try to get request ID from headers first
    request_id = request.headers.get("X-Request-ID")
    if not request_id:
        # Generate a new request ID
        request_id = str(uuid.uuid4())
    return request_id


async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Global exception handler for unhandled exceptions."""
    request_id = get_request_id(request)
    
    # Log the unhandled exception
    logger.error(f"Unhandled exception for request {request_id}: {str(exc)}", exc_info=True)
    
    # Create standardized error response
    error = create_system_error(
        component="global_handler",
        request_id=request_id,
        include_debug_info=False  # Never expose debug info in production
    )
    
    return JSONResponse(
        status_code=500,
        content=error.to_dict(),
        headers={"X-Request-ID": request_id}
    )


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Handler for HTTPExceptions to ensure consistent error format."""
    request_id = get_request_id(request)
    
    # Check if the exception already has standardized error format
    if isinstance(exc.detail, dict) and "error" in exc.detail:
        # Already standardized, just add request ID header
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.detail,
            headers={**exc.headers, "X-Request-ID": request_id} if exc.headers else {"X-Request-ID": request_id}
        )
    
    # Convert non-standardized HTTPException to standardized format
    error = StandardizedError(
        code=ErrorCode.SYSTEM_ERROR,
        message=str(exc.detail),
        request_id=request_id
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content=error.to_dict(),
        headers={**exc.headers, "X-Request-ID": request_id} if exc.headers else {"X-Request-ID": request_id}
    )


async def validation_exception_handler(request: Request, exc: ValidationError) -> JSONResponse:
    """Handler for Pydantic validation errors."""
    request_id = get_request_id(request)
    
    error = handle_pydantic_validation_error(exc, request_id)
    
    return JSONResponse(
        status_code=400,
        content=error.to_dict(),
        headers={"X-Request-ID": request_id}
    )