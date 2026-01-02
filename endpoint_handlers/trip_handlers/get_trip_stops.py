
from typing import List
from database_connector import DatabaseConnector
from pydantic_models import TripStop

def get_trip_stops(db: DatabaseConnector, trip_id: str) -> List[TripStop]:
    """
    Get the complete stop sequence for a specific trip.

    Args:
        db: Database connector instance
        trip_id: Unique identifier for the trip

    Returns:
        List of TripStop objects ordered by stop sequence
    """
    query = """
            SELECT
                st.stop_id,
                s.stop_name,
                st.arrival_time,
                st.departure_time,
                st.stop_sequence
            FROM stop_times st
                     JOIN stops s ON st.stop_id = s.stop_id
            WHERE st.trip_id = ?
            ORDER BY st.stop_sequence \
            """

    df = db.execute_df(query, [trip_id])

    trip_stops = []
    for _, row in df.iterrows():
        trip_stop = TripStop(
            stop_id=row['stop_id'],
            stop_name=row['stop_name'],
            arrival_time=row['arrival_time'],
            departure_time=row['departure_time'],
            stop_sequence=int(row['stop_sequence'])
        )
        trip_stops.append(trip_stop)

    return trip_stops