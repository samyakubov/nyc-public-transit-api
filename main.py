from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List

from endpoints.routes import route_routes

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class RouteFeature(BaseModel):
    type: str = "Feature"
    properties: dict
    geometry: dict

class GeoJSONResponse(BaseModel):
    type: str = "FeatureCollection"
    features: List[RouteFeature]


app.include_router(route_routes)