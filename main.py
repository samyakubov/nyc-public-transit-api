from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from endpoints.routes import route_routes

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(route_routes)