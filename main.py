from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from contextlib import asynccontextmanager
from pydantic import ValidationError
from endpoints.routes import route_routes
from endpoints.stops import stop_routes
from endpoints.trips import trip_routes
from endpoints.system import router as system_router
from utils.cache_management import get_cache_manager
from utils.cache_middleware import add_cache_middleware
from utils.error_handling import (
    global_exception_handler,
    http_exception_handler,
    validation_exception_handler
)
from utils.rate_limiting import add_rate_limiting_middleware
from database_connector import get_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager for cache warming and cleanup.
    """
    # Startup: Warm the cache with frequently accessed data
    try:
        cache_manager = get_cache_manager()
        db = next(get_db())  # Get database connection
        warmed_counts = await cache_manager.warm_cache(db)
        print(f"Cache warmed on startup: {warmed_counts}")
    except Exception as e:
        print(f"Cache warming failed on startup: {e}")
    
    yield
    
    # Shutdown: Clean up cache
    try:
        cache_manager = get_cache_manager()
        cache_manager.invalidate_all_cache()
        print("Cache cleared on shutdown")
    except Exception as e:
        print(f"Cache cleanup failed on shutdown: {e}")


app = FastAPI(
    title="NYC Public Transit API",
    description="Comprehensive transit data access API with stops, routes, trips, journey planning, and system status",
    version="2.0.0",
    lifespan=lifespan
)

# Add comprehensive error handling
app.add_exception_handler(Exception, global_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(ValidationError, validation_exception_handler)

# Add rate limiting middleware (before cache middleware)
add_rate_limiting_middleware(app, exclude_paths=["/docs", "/redoc", "/openapi.json", "/health", "/system/status"])

# Add cache middleware for automatic cleanup and monitoring
add_cache_middleware(app, cleanup_interval=300)  # 5 minutes

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(route_routes)
app.include_router(stop_routes)
app.include_router(trip_routes)
app.include_router(system_router)