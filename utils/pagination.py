"""
Pagination utilities for handling large datasets in the transit API.
Supports both offset-based and cursor-based pagination patterns.
"""

from typing import Generic, TypeVar, List, Optional, Dict, Any
from pydantic import BaseModel
from math import ceil

T = TypeVar('T')


class PaginationParams(BaseModel):
    """Standard pagination parameters for API requests."""
    page: int = 1
    per_page: int = 20
    max_per_page: int = 100
    
    def __post_init__(self):
        """Validate pagination parameters."""
        if self.page < 1:
            self.page = 1
        if self.per_page < 1:
            self.per_page = 20
        if self.per_page > self.max_per_page:
            self.per_page = self.max_per_page


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response structure."""
    items: List[T]
    total: int
    page: int
    per_page: int
    total_pages: int
    has_next: bool
    has_prev: bool
    next_page: Optional[int] = None
    prev_page: Optional[int] = None


class CursorPaginationParams(BaseModel):
    """Cursor-based pagination parameters for large datasets."""
    cursor: Optional[str] = None
    limit: int = 20
    max_limit: int = 100
    
    def __post_init__(self):
        """Validate cursor pagination parameters."""
        if self.limit < 1:
            self.limit = 20
        if self.limit > self.max_limit:
            self.limit = self.max_limit


class CursorPaginatedResponse(BaseModel, Generic[T]):
    """Cursor-based paginated response structure."""
    items: List[T]
    has_more: bool
    next_cursor: Optional[str] = None
    prev_cursor: Optional[str] = None


def create_paginated_response(
    items: List[T], 
    total: int, 
    page: int, 
    per_page: int
) -> PaginatedResponse[T]:
    """
    Create a paginated response from a list of items.
    
    Args:
        items: List of items for current page
        total: Total number of items across all pages
        page: Current page number (1-based)
        per_page: Number of items per page
    
    Returns:
        PaginatedResponse with metadata
    """
    total_pages = ceil(total / per_page) if per_page > 0 else 1
    has_next = page < total_pages
    has_prev = page > 1
    
    return PaginatedResponse(
        items=items,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        has_next=has_next,
        has_prev=has_prev,
        next_page=page + 1 if has_next else None,
        prev_page=page - 1 if has_prev else None
    )


def calculate_offset(page: int, per_page: int) -> int:
    """
    Calculate the offset for database queries based on page and per_page.
    
    Args:
        page: Page number (1-based)
        per_page: Items per page
    
    Returns:
        Offset for database query (0-based)
    """
    return (page - 1) * per_page


def create_cursor_response(
    items: List[T],
    has_more: bool,
    next_cursor: Optional[str] = None,
    prev_cursor: Optional[str] = None
) -> CursorPaginatedResponse[T]:
    """
    Create a cursor-based paginated response.
    
    Args:
        items: List of items for current page
        has_more: Whether there are more items available
        next_cursor: Cursor for next page
        prev_cursor: Cursor for previous page
    
    Returns:
        CursorPaginatedResponse with navigation cursors
    """
    return CursorPaginatedResponse(
        items=items,
        has_more=has_more,
        next_cursor=next_cursor,
        prev_cursor=prev_cursor
    )


def generate_pagination_metadata(
    total: int,
    page: int,
    per_page: int,
    base_url: str,
    query_params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Generate pagination metadata including navigation URLs.
    
    Args:
        total: Total number of items
        page: Current page number
        per_page: Items per page
        base_url: Base URL for pagination links
        query_params: Additional query parameters to preserve
    
    Returns:
        Dictionary with pagination metadata and navigation URLs
    """
    total_pages = ceil(total / per_page) if per_page > 0 else 1
    has_next = page < total_pages
    has_prev = page > 1
    
    # Build query string
    params = query_params.copy() if query_params else {}
    params['per_page'] = per_page
    
    def build_url(page_num: int) -> str:
        params['page'] = page_num
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        return f"{base_url}?{query_string}"
    
    metadata = {
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': total_pages,
        'has_next': has_next,
        'has_prev': has_prev,
    }
    
    if has_next:
        metadata['next_url'] = build_url(page + 1)
    if has_prev:
        metadata['prev_url'] = build_url(page - 1)
    if total_pages > 0:
        metadata['first_url'] = build_url(1)
        metadata['last_url'] = build_url(total_pages)
    
    return metadata