from fastapi import APIRouter
from database_connector import get_db, DatabaseConnector
from fastapi import Query, Depends

from endpoint_handlers.get_nearby_routes import get_nearby_routes

route_routes = APIRouter(prefix="/routes")

@route_routes.get("/nearby")
def get_nearby(
            lat: float = Query(..., description="Latitude"),
            lon: float = Query(..., description="Longitude"),
            radius_miles: float = Query(0.5, description="Search radius in miles"),
            db: DatabaseConnector = Depends(get_db)):

    return get_nearby_routes(db, lat, lon, radius_miles)


