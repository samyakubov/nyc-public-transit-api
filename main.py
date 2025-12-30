from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import duckdb
import json
from pathlib import Path
import uvicorn

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


con = None

class RouteFeature(BaseModel):
    type: str = "Feature"
    properties: dict
    geometry: dict

class GeoJSONResponse(BaseModel):
    type: str = "FeatureCollection"
    features: List[RouteFeature]

@app.get("/routes", response_model=GeoJSONResponse)
async def get_all_routes(
        route_name: Optional[str] = Query(None, description="Filter by route name (e.g., M15)"),
        limit: Optional[int] = Query(None, description="Limit number of routes")
):
    """Get all bus routes as GeoJSON"""
    db = get_db()

    query = "SELECT * FROM bus_routes"
    params = []

    if route_name:
        query += " WHERE route_short_name = ?"
        params.append(route_name)

    if limit:
        query += f" LIMIT {limit}"

    results = db.execute(query, params).fetchall()
    columns = [desc[0] for desc in db.description]

    features = []
    for row in results:
        row_dict = dict(zip(columns, row))

        # Convert coordinates from list of dicts to GeoJSON format
        coords = [[c['lon'], c['lat']] for c in row_dict['coordinates']]

        feature = {
            "type": "Feature",
            "properties": {
                "routeId": row_dict['route_id'],
                "routeName": row_dict['route_short_name'],
                "routeLongName": row_dict['route_long_name'],
                "routeColor": f"#{row_dict['route_color']}" if row_dict['route_color'] else "#0039A6",
                "routeTextColor": f"#{row_dict['route_text_color']}" if row_dict['route_text_color'] else "#FFFFFF",
                "shapeId": row_dict['shape_id']
            },
            "geometry": {
                "type": "LineString",
                "coordinates": coords
            }
        }
        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features
    }

@app.get("/routes/{route_name}")
async def get_route_by_name(route_name: str):
    """Get a specific bus route by name"""
    db = get_db()

    results = db.execute(
        "SELECT * FROM bus_routes WHERE route_short_name = ?",
        [route_name]
    ).fetchall()

    if not results:
        raise HTTPException(status_code=404, detail=f"Route {route_name} not found")

    columns = [desc[0] for desc in db.description]
    features = []

    for row in results:
        row_dict = dict(zip(columns, row))
        coords = [[c['lon'], c['lat']] for c in row_dict['coordinates']]

        feature = {
            "type": "Feature",
            "properties": {
                "routeId": row_dict['route_id'],
                "routeName": row_dict['route_short_name'],
                "routeLongName": row_dict['route_long_name'],
                "routeColor": f"#{row_dict['route_color']}",
                "routeTextColor": f"#{row_dict['route_text_color']}",
                "shapeId": row_dict['shape_id']
            },
            "geometry": {
                "type": "LineString",
                "coordinates": coords
            }
        }
        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features
    }

@app.get("/routes/nearby/point")
async def get_nearby_routes(
        lat: float = Query(..., description="Latitude"),
        lon: float = Query(..., description="Longitude"),
        radius_miles: float = Query(0.5, description="Search radius in miles")
):
    """Get bus routes within radius of a point"""
    db = get_db()

    # Convert miles to degrees (approximate)
    radius_deg = radius_miles / 69.0

    # Query routes where any point is within bounding box
    # This is approximate but fast
    query = """
            WITH nearby AS (
                SELECT
                    route_id,
                    route_short_name,
                    route_long_name,
                    route_color,
                    route_text_color,
                    shape_id,
                    coordinates,
                    (SELECT MIN(
                                    SQRT(POW(c.lon - ?, 2) + POW(c.lat - ?, 2))
                            ) FROM UNNEST(coordinates) AS c) as min_distance
                FROM bus_routes
            )
            SELECT * FROM nearby
            WHERE min_distance < ?
            ORDER BY min_distance \
            """

    results = db.execute(query, [lon, lat, radius_deg]).fetchall()
    columns = [desc[0] for desc in db.description]

    features = []
    for row in results:
        row_dict = dict(zip(columns, row))
        coords = [[c['lon'], c['lat']] for c in row_dict['coordinates']]

        feature = {
            "type": "Feature",
            "properties": {
                "routeId": row_dict['route_id'],
                "routeName": row_dict['route_short_name'],
                "routeLongName": row_dict['route_long_name'],
                "routeColor": f"#{row_dict['route_color']}",
                "routeTextColor": f"#{row_dict['route_text_color']}",
                "shapeId": row_dict['shape_id'],
                "distance": round(row_dict['min_distance'] * 69, 2)  # Convert back to miles
            },
            "geometry": {
                "type": "LineString",
                "coordinates": coords
            }
        }
        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features,
        "query": {
            "lat": lat,
            "lon": lon,
            "radius_miles": radius_miles,
            "count": len(features)
        }
    }
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)