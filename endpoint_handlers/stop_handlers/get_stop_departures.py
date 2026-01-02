from typing import List
from fastapi import HTTPException
from database_connector import DatabaseConnector
from pydantic_models import StopDeparture
from datetime import datetime, timedelta
from utils.caching import cached


@cached(ttl=60)
def get_stop_departures_handler(
        db: DatabaseConnector,
        stop_id: str,
        limit: int,
        time_window_hours: int
) -> List[StopDeparture]:
    """
    Get upcoming departures from a specific stop.

    Returns next departures sorted chronologically within the specified time window.
    Uses actual departure times from the stop_times table.
    """
    try:
        stop_check_query = "SELECT COUNT(*) as count FROM stops WHERE stop_id = ?"
        stop_count = db.execute_df(stop_check_query, [stop_id])

        if stop_count.iloc[0]['count'] == 0:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "code": "STOP_NOT_FOUND",
                        "message": f"Stop with ID '{stop_id}' not found",
                        "details": {
                            "field": "stop_id",
                            "value": stop_id,
                            "constraint": "must be a valid stop identifier"
                        }
                    }
                }
            )

        current_time = datetime.now()
        start_time = current_time.strftime("%H:%M:%S")
        end_time = (current_time + timedelta(hours=time_window_hours)).strftime("%H:%M:%S")

        if time_window_hours > 24 - current_time.hour:
            end_time = "23:59:59"

        query = """
                SELECT
                    t.trip_id,
                    t.route_id,
                    r.route_short_name,
                    r.route_long_name,
                    t.trip_headsign,
                    st.departure_time,
                    st.stop_sequence
                FROM stop_times st
                         JOIN trips t ON st.trip_id = t.trip_id
                         JOIN routes r ON t.route_id = r.route_id
                WHERE st.stop_id = ?
                  AND st.departure_time BETWEEN ? AND ?
                ORDER BY st.departure_time, st.stop_sequence
                    LIMIT ? \
                """

        df = db.execute_df(query, [stop_id, start_time, end_time, limit])

        departures = []
        for _, row in df.iterrows():
            departure = StopDeparture(
                trip_id=row['trip_id'],
                route_id=row['route_id'],
                route_short_name=row['route_short_name'] or '',
                route_long_name=row['route_long_name'] or '',
                headsign=row.get('trip_headsign'),
                departure_time=row['departure_time'],
            )
            departures.append(departure)

        return departures

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving stop departures: {str(e)}")