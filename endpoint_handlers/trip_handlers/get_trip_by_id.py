from typing import Optional
from database_connector import DatabaseConnector
from pydantic_models import Trip


def get_trip_by_id(db: DatabaseConnector, trip_id: str) -> Optional[Trip]:
    """
    Get detailed information about a specific trip.

    Args:
        db: Database connector instance
        trip_id: Unique identifier for the trip

    Returns:
        Trip object or None if not found
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
            WHERE trip_id = ? \
            """

    df = db.execute_df(query, [trip_id])

    if df.empty:
        return None

    row = df.iloc[0]

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

    return trip