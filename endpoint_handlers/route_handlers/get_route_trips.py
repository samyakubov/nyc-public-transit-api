from typing import Optional, List

from database_connector import DatabaseConnector
from models.pydantic_models import Trip
from utils.caching import cached


@cached(ttl=300)
def get_route_trips(db: DatabaseConnector, route_id: str, service_date: Optional[str] = None, limit: int = 100) -> List[Trip]:
    """
    Get all trips for a specific route.

    Args:
        db: Database connector instance
        route_id: Unique identifier for the route
        service_date: Optional service date filter in YYYY-MM-DD format (not used without calendar table)
        limit: Maximum number of trips to return

    Returns:
        List of Trip objects
    """
    query = """
            SELECT
                trip_id,
                route_id,
                service_id,
                trip_headsign,
                direction_id,
                shape_id
            FROM trips
            WHERE route_id = ?
            ORDER BY trip_headsign, direction_id
                LIMIT ? \
            """
    params = [route_id, limit]

    df = db.execute_df(query, params)

    trips = []
    for _, row in df.iterrows():
        # Convert direction_id to int if present
        direction_id = None
        if row.get('direction_id') and str(row['direction_id']).strip():
            try:
                direction_id = int(row['direction_id'])
            except (ValueError, TypeError):
                direction_id = None

        trip = Trip(
            trip_id=row['trip_id'],
            route_id=row['route_id'],
            service_id=row['service_id'],
            trip_headsign=row.get('trip_headsign'),
            direction_id=direction_id,
            shape_id=row.get('shape_id')
        )
        trips.append(trip)

    return trips