"""
System status endpoint handlers.
Implements business logic for system status, alerts, and statistics operations.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from database_connector import DatabaseConnector
from pydantic_models import SystemStatus, ServiceAlert
from utils.caching import cached


@cached(ttl=60)  # Cache for 1 minute - system status should be relatively fresh
def get_system_status(db: DatabaseConnector) -> SystemStatus:
    """
    Get overall system status information.
    
    Args:
        db: Database connector instance
        
    Returns:
        SystemStatus object with current system health
    """
    try:
        # Get total number of routes
        total_routes_query = "SELECT COUNT(*) as total FROM routes"
        total_routes_df = db.execute_df(total_routes_query)
        total_routes = int(total_routes_df.iloc[0]['total'])
        
        # For this implementation, assume all routes are operational
        # In a real system, this would check actual service status
        routes_operational = total_routes
        
        # Get number of active alerts (simulated)
        active_alerts = len(get_active_alerts(db))
        
        # Determine overall system status
        if active_alerts == 0:
            status = "operational"
        elif active_alerts <= 3:
            status = "degraded"
        else:
            status = "down"
        
        return SystemStatus(
            status=status,
            last_updated=datetime.now().isoformat(),
            active_alerts=active_alerts,
            routes_operational=routes_operational,
            total_routes=total_routes
        )
        
    except Exception as e:
        # Return degraded status if we can't determine system health
        return SystemStatus(
            status="degraded",
            last_updated=datetime.now().isoformat(),
            active_alerts=0,
            routes_operational=0,
            total_routes=0
        )


@cached(ttl=120)  # Cache for 2 minutes - alerts change but not too frequently
def get_active_alerts(db: DatabaseConnector, severity: Optional[str] = None) -> List[ServiceAlert]:
    """
    Get current active service alerts with enhanced categorization and filtering.
    
    Args:
        db: Database connector instance
        severity: Optional filter by severity level (info, warning, severe)
        
    Returns:
        List of ServiceAlert objects
    """
    # Since we don't have an alerts table, simulate comprehensive alerts based on routes and stops
    # In a real system, this would query an alerts/service_notices table
    
    try:
        # Get sample routes and stops to create realistic mock alerts
        routes_query = """
        SELECT route_id, route_short_name, route_long_name, route_type
        FROM routes 
        ORDER BY route_short_name 
        LIMIT 10
        """
        routes_df = db.execute_df(routes_query)
        
        stops_query = """
        SELECT stop_id, stop_name 
        FROM stops 
        ORDER BY stop_name 
        LIMIT 5
        """
        stops_df = db.execute_df(stops_query)
        
        alerts = []
        current_time = datetime.now()
        
        # Create comprehensive mock alerts with proper categorization
        if not routes_df.empty:
            # Severe alerts - Service suspensions and major disruptions
            route1 = routes_df.iloc[0]
            alerts.append(ServiceAlert(
                alert_id=f"alert_{route1['route_id']}_severe_001",
                severity="severe",
                title=f"Service Suspension - {route1['route_short_name']}",
                description=f"Service on {route1['route_short_name']} {route1['route_long_name']} is temporarily suspended due to signal problems. Use alternative routes.",
                affected_routes=[route1['route_id']],
                affected_stops=_get_stops_for_route(db, route1['route_id'])[:3],  # First 3 stops
                start_time=(current_time - timedelta(hours=2)).isoformat(),
                end_time=(current_time + timedelta(hours=6)).isoformat()
            ))
            
            # Warning alerts - Delays and service changes
            if len(routes_df) > 1:
                route2 = routes_df.iloc[1]
                alerts.append(ServiceAlert(
                    alert_id=f"alert_{route2['route_id']}_warning_002",
                    severity="warning",
                    title=f"Delays - {route2['route_short_name']}",
                    description=f"Expect delays of 15-20 minutes on {route2['route_short_name']} {route2['route_long_name']} due to track maintenance.",
                    affected_routes=[route2['route_id']],
                    affected_stops=_get_stops_for_route(db, route2['route_id'])[:2],
                    start_time=(current_time - timedelta(minutes=45)).isoformat(),
                    end_time=(current_time + timedelta(hours=3)).isoformat()
                ))
            
            # Info alerts - Planned service changes and announcements
            if len(routes_df) > 2:
                route3 = routes_df.iloc[2]
                alerts.append(ServiceAlert(
                    alert_id=f"alert_{route3['route_id']}_info_003",
                    severity="info",
                    title=f"Schedule Change - {route3['route_short_name']}",
                    description=f"{route3['route_short_name']} will operate on a modified schedule this weekend for planned maintenance work.",
                    affected_routes=[route3['route_id']],
                    affected_stops=[],
                    start_time=(current_time + timedelta(days=1)).isoformat(),
                    end_time=(current_time + timedelta(days=3)).isoformat()
                ))
            
            # System-wide alerts affecting multiple routes
            if len(routes_df) > 3:
                affected_routes = [route['route_id'] for _, route in routes_df.head(4).iterrows()]
                alerts.append(ServiceAlert(
                    alert_id="alert_system_multi_004",
                    severity="warning",
                    title="Weekend Service Changes",
                    description="Multiple routes will have modified schedules this weekend. Check individual route information for details.",
                    affected_routes=affected_routes,
                    affected_stops=[],
                    start_time=(current_time + timedelta(days=2)).isoformat(),
                    end_time=(current_time + timedelta(days=4)).isoformat()
                ))
            
            # Stop-specific alerts
            if not stops_df.empty and len(routes_df) > 4:
                stop1 = stops_df.iloc[0]
                route5 = routes_df.iloc[4] if len(routes_df) > 4 else routes_df.iloc[0]
                alerts.append(ServiceAlert(
                    alert_id=f"alert_stop_{stop1['stop_id']}_005",
                    severity="info",
                    title=f"Station Improvement - {stop1['stop_name']}",
                    description=f"Accessibility improvements are being made at {stop1['stop_name']}. Some entrances may be temporarily closed.",
                    affected_routes=[route5['route_id']],
                    affected_stops=[stop1['stop_id']],
                    start_time=(current_time - timedelta(days=1)).isoformat(),
                    end_time=(current_time + timedelta(days=7)).isoformat()
                ))
            
            # Emergency alert (severe, immediate)
            if len(routes_df) > 5:
                route6 = routes_df.iloc[5]
                alerts.append(ServiceAlert(
                    alert_id=f"alert_{route6['route_id']}_emergency_006",
                    severity="severe",
                    title=f"Emergency Service Disruption - {route6['route_short_name']}",
                    description=f"Emergency situation affecting {route6['route_short_name']}. Service is suspended until further notice. Seek alternative transportation.",
                    affected_routes=[route6['route_id']],
                    affected_stops=_get_stops_for_route(db, route6['route_id'])[:5],
                    start_time=(current_time - timedelta(minutes=15)).isoformat(),
                    end_time=None  # Open-ended emergency alert
                ))
        
        # Filter by severity if specified
        if severity:
            alerts = [alert for alert in alerts if alert.severity == severity]
        
        # Filter to only active alerts based on current time
        active_alerts = []
        for alert in alerts:
            if _is_alert_active(alert, current_time):
                active_alerts.append(alert)
        
        # Sort alerts by severity (severe first, then warning, then info) and start time
        severity_order = {'severe': 0, 'warning': 1, 'info': 2}
        active_alerts.sort(key=lambda x: (
            severity_order.get(x.severity, 3),
            x.start_time or ''
        ))
        
        return active_alerts
        
    except Exception as e:
        # Return empty list if we can't get alerts
        return []


def _get_stops_for_route(db: DatabaseConnector, route_id: str) -> List[str]:
    """
    Helper function to get stop IDs for a route.
    
    Args:
        db: Database connector instance
        route_id: Route identifier
        
    Returns:
        List of stop IDs served by the route
    """
    try:
        query = """
        SELECT DISTINCT s.stop_id
        FROM stops s
        JOIN stop_times st ON s.stop_id = st.stop_id
        JOIN trips t ON st.trip_id = t.trip_id
        WHERE t.route_id = ?
        LIMIT 10
        """
        df = db.execute_df(query, [route_id])
        return [row['stop_id'] for _, row in df.iterrows()]
    except Exception:
        return []


def _is_alert_active(alert: ServiceAlert, current_time: datetime) -> bool:
    """
    Check if an alert is currently active based on start and end times.
    
    Args:
        alert: ServiceAlert object
        current_time: Current datetime
        
    Returns:
        True if alert is active, False otherwise
    """
    try:
        # If no start time, assume it started in the past
        start_time = datetime.fromisoformat(alert.start_time) if alert.start_time else current_time - timedelta(days=1)
        
        # If no end time, assume it's ongoing (especially for emergency alerts)
        end_time = datetime.fromisoformat(alert.end_time) if alert.end_time else current_time + timedelta(days=1)
        
        return start_time <= current_time <= end_time
    except Exception:
        # If we can't parse times, assume alert is active
        return True


def get_alerts_by_type(db: DatabaseConnector, alert_type: str) -> List[ServiceAlert]:
    """
    Get alerts categorized by type (delay, suspension, detour, maintenance, emergency).
    
    Args:
        db: Database connector instance
        alert_type: Type of alert to filter by
        
    Returns:
        List of ServiceAlert objects of the specified type
    """
    all_alerts = get_active_alerts(db)
    
    # Categorize alerts by type based on title and description keywords
    type_keywords = {
        'delay': ['delay', 'delayed', 'slower'],
        'suspension': ['suspension', 'suspended', 'not running', 'cancelled'],
        'detour': ['detour', 'rerouted', 'alternate route'],
        'maintenance': ['maintenance', 'work', 'improvement', 'construction'],
        'emergency': ['emergency', 'incident', 'police', 'medical']
    }
    
    if alert_type not in type_keywords:
        return []
    
    keywords = type_keywords[alert_type]
    filtered_alerts = []
    
    for alert in all_alerts:
        title_lower = alert.title.lower()
        desc_lower = alert.description.lower()
        
        if any(keyword in title_lower or keyword in desc_lower for keyword in keywords):
            filtered_alerts.append(alert)
    
    return filtered_alerts


def get_alerts_by_time_range(db: DatabaseConnector, start_time: str, end_time: str) -> List[ServiceAlert]:
    """
    Get alerts active within a specific time range.
    
    Args:
        db: Database connector instance
        start_time: Start time in ISO format
        end_time: End time in ISO format
        
    Returns:
        List of ServiceAlert objects active in the time range
    """
    try:
        start_dt = datetime.fromisoformat(start_time)
        end_dt = datetime.fromisoformat(end_time)
        
        all_alerts = get_active_alerts(db)
        time_filtered_alerts = []
        
        for alert in all_alerts:
            alert_start = datetime.fromisoformat(alert.start_time) if alert.start_time else start_dt
            alert_end = datetime.fromisoformat(alert.end_time) if alert.end_time else end_dt
            
            # Check if alert time range overlaps with requested time range
            if alert_start <= end_dt and alert_end >= start_dt:
                time_filtered_alerts.append(alert)
        
        return time_filtered_alerts
        
    except Exception:
        return []


@cached(ttl=300)  # Cache for 5 minutes - system stats don't change frequently
def get_system_stats(db: DatabaseConnector) -> Dict[str, Any]:
    """
    Get system usage and coverage statistics.
    
    Args:
        db: Database connector instance
        
    Returns:
        Dictionary with system statistics
    """
    try:
        stats = {}
        
        # Get route statistics
        routes_query = "SELECT COUNT(*) as total_routes FROM routes"
        routes_df = db.execute_df(routes_query)
        stats['total_routes'] = int(routes_df.iloc[0]['total_routes'])
        
        # Get stop statistics
        stops_query = "SELECT COUNT(*) as total_stops FROM stops"
        stops_df = db.execute_df(stops_query)
        stats['total_stops'] = int(stops_df.iloc[0]['total_stops'])
        
        # Get trip statistics
        trips_query = "SELECT COUNT(*) as total_trips FROM trips"
        trips_df = db.execute_df(trips_query)
        stats['total_trips'] = int(trips_df.iloc[0]['total_trips'])
        
        # Get route type breakdown
        route_types_query = """
        SELECT 
            route_type,
            COUNT(*) as count
        FROM routes 
        GROUP BY route_type
        ORDER BY count DESC
        """
        route_types_df = db.execute_df(route_types_query)
        
        route_type_names = {
            '0': 'Tram/Light Rail',
            '1': 'Subway/Metro',
            '2': 'Rail',
            '3': 'Bus',
            '4': 'Ferry',
            '5': 'Cable Tram',
            '6': 'Aerial Lift',
            '7': 'Funicular'
        }
        
        route_breakdown = {}
        for _, row in route_types_df.iterrows():
            route_type = str(row['route_type'])
            type_name = route_type_names.get(route_type, f'Type {route_type}')
            route_breakdown[type_name] = int(row['count'])
        
        stats['route_types'] = route_breakdown
        
        # Calculate coverage area (approximate bounding box)
        coverage_query = """
        SELECT 
            MIN(CAST(stop_lat AS DOUBLE)) as min_lat,
            MAX(CAST(stop_lat AS DOUBLE)) as max_lat,
            MIN(CAST(stop_lon AS DOUBLE)) as min_lon,
            MAX(CAST(stop_lon AS DOUBLE)) as max_lon
        FROM stops
        """
        coverage_df = db.execute_df(coverage_query)
        
        if not coverage_df.empty:
            coverage_row = coverage_df.iloc[0]
            stats['coverage_area'] = {
                'min_latitude': float(coverage_row['min_lat']),
                'max_latitude': float(coverage_row['max_lat']),
                'min_longitude': float(coverage_row['min_lon']),
                'max_longitude': float(coverage_row['max_lon'])
            }
        
        # Add timestamp
        stats['last_updated'] = datetime.now().isoformat()
        
        return stats
        
    except Exception as e:
        # Return basic stats if detailed query fails
        return {
            'total_routes': 0,
            'total_stops': 0,
            'total_trips': 0,
            'route_types': {},
            'coverage_area': {},
            'last_updated': datetime.now().isoformat(),
            'error': 'Unable to retrieve detailed statistics'
        }


def get_alerts_for_route(db: DatabaseConnector, route_id: str) -> List[ServiceAlert]:
    """
    Get active alerts for a specific route.
    
    Args:
        db: Database connector instance
        route_id: Route identifier to get alerts for
        
    Returns:
        List of ServiceAlert objects affecting the route
    """
    all_alerts = get_active_alerts(db)
    route_alerts = [alert for alert in all_alerts if route_id in alert.affected_routes]
    return route_alerts


def get_alerts_for_stop(db: DatabaseConnector, stop_id: str) -> List[ServiceAlert]:
    """
    Get active alerts for a specific stop.
    
    Args:
        db: Database connector instance
        stop_id: Stop identifier to get alerts for
        
    Returns:
        List of ServiceAlert objects affecting the stop
    """
    all_alerts = get_active_alerts(db)
    stop_alerts = [alert for alert in all_alerts if stop_id in alert.affected_stops]
    
    # Also include alerts for routes serving this stop
    try:
        routes_query = """
        SELECT DISTINCT t.route_id
        FROM trips t
        JOIN stop_times st ON t.trip_id = st.trip_id
        WHERE st.stop_id = ?
        """
        routes_df = db.execute_df(routes_query, [stop_id])
        
        route_ids = [row['route_id'] for _, row in routes_df.iterrows()]
        
        for alert in all_alerts:
            if any(route_id in alert.affected_routes for route_id in route_ids):
                if alert not in stop_alerts:
                    stop_alerts.append(alert)
    
    except Exception:
        pass
    
    return stop_alerts