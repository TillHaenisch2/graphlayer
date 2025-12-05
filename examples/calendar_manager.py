#!/usr/bin/env python3
"""
Calendar Management System - iCal to ObjectStore + GraphLayer
Imports calendar events with hierarchical date structure:
Year → Month → Week → Day → Event
"""

import json
import sys
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
from collections import defaultdict


# ============================================================================
# ObjectStore Client
# ============================================================================

class ObjectStoreClient:
    """Client for ObjectStore REST API"""
    
    def __init__(self, base_url: str = "http://localhost:5000", api_token: str = ""):
        self.base_url = base_url
        self.session = requests.Session()
        if api_token:
            self.session.headers['X-API-Token'] = api_token
    
    def store(self, data: bytes, content_type: str = "application/json",
              metadata: Dict = None) -> Dict:
        """Store an object"""
        headers = {'Content-Type': content_type}
        if metadata:
            headers['X-Metadata'] = json.dumps(metadata)
        
        response = self.session.post(
            f"{self.base_url}/api/v1/objects",
            data=data,
            headers=headers
        )
        response.raise_for_status()
        return response.json()
    
    def get_object_url(self, object_id: str) -> str:
        """Get URL for object"""
        return f"{self.base_url}/api/v1/objects/{object_id}"


# ============================================================================
# GraphLayer Client
# ============================================================================

class GraphClient:
    """Client for GraphLayer REST API"""
    
    def __init__(self, base_url: str = "http://localhost:5001", token: str = ""):
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def _get(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        """GET request"""
        url = f"{self.base_url}/api/v1{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def _post(self, endpoint: str, data: Dict) -> Dict[str, Any]:
        """POST request"""
        url = f"{self.base_url}/api/v1{endpoint}"
        response = requests.post(url, headers=self.headers, json=data)
        response.raise_for_status()
        return response.json()
    
    def register_schema(self, class_name: str, parent_class: str = "Thing",
                       attributes: Dict = None, description: str = "") -> Dict:
        """Register a schema"""
        data = {
            "class_name": class_name,
            "parent_class": parent_class,
            "attributes": attributes or {},
            "description": description
        }
        return self._post("/schemas", data)
    
    def create_node(self, class_name: str, name: str, attributes: Dict = None) -> Dict:
        """Create a node"""
        data = {
            "class_name": class_name,
            "name": name,
            "attributes": attributes or {}
        }
        return self._post("/nodes", data)
    
    def create_edge(self, from_node_id: str, to_node_id: str,
                   edge_type: str = "contains", attributes: Dict = None) -> Dict:
        """Create an edge"""
        data = {
            "from_node_id": from_node_id,
            "to_node_id": to_node_id,
            "edge_type": edge_type,
            "attributes": attributes or {}
        }
        return self._post("/edges", data)
    
    def get_related_nodes(self, node_id: str, direction: str = "outgoing") -> Dict:
        """Get related nodes"""
        return self._get(f"/query/related/{node_id}", params={"direction": direction})
    
    def filter_by_class(self, class_name: str, filter_data: Dict = None) -> Dict:
        """Filter nodes by class
        
        Args:
            class_name: The class to filter
            filter_data: Complete filter data structure (should already have 'filter' key if needed)
        
        Returns:
            API response with nodes
        """
        data = filter_data if filter_data else {}
        return self._post(f"/filter/by-class/{class_name}", data)


# ============================================================================
# Calendar Parser
# ============================================================================

def parse_ical_file(filepath: str) -> List[Dict]:
    """Parse iCal file and extract events"""
    try:
        from icalendar import Calendar
    except ImportError:
        print("Installing icalendar library...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", 
                             "icalendar", "--break-system-packages"])
        from icalendar import Calendar
    
    import pytz
    
    events = []
    
    # Define the target timezone (Europe/Berlin as per the calendar)
    local_tz = pytz.timezone('Europe/Berlin')
    
    with open(filepath, 'rb') as f:
        cal = Calendar.from_ical(f.read())
        
        for component in cal.walk():
            if component.name == "VEVENT":
                event = {}
                
                # Extract basic fields
                event['uid'] = str(component.get('uid', ''))
                event['summary'] = str(component.get('summary', 'Untitled Event'))
                event['description'] = str(component.get('description', ''))
                event['status'] = str(component.get('status', 'CONFIRMED'))
                
                # Extract dates with timezone handling
                dtstart = component.get('dtstart')
                dtend = component.get('dtend')
                
                if dtstart:
                    start_dt = dtstart.dt
                    if isinstance(start_dt, datetime):
                        # If timezone-aware, convert to local timezone
                        if start_dt.tzinfo is not None:
                            start_dt = start_dt.astimezone(local_tz)
                        else:
                            # If naive, assume it's already in local timezone
                            start_dt = local_tz.localize(start_dt)
                        event['start'] = start_dt
                    else:
                        # All-day event (date object)
                        event['start'] = local_tz.localize(datetime.combine(start_dt, datetime.min.time()))
                    
                if dtend:
                    end_dt = dtend.dt
                    if isinstance(end_dt, datetime):
                        # If timezone-aware, convert to local timezone
                        if end_dt.tzinfo is not None:
                            end_dt = end_dt.astimezone(local_tz)
                        else:
                            # If naive, assume it's already in local timezone
                            end_dt = local_tz.localize(end_dt)
                        event['end'] = end_dt
                    else:
                        # All-day event (date object)
                        event['end'] = local_tz.localize(datetime.combine(end_dt, datetime.min.time()))
                
                # Calculate duration
                if 'start' in event and 'end' in event:
                    duration = event['end'] - event['start']
                    event['duration_minutes'] = int(duration.total_seconds() / 60)
                
                # Timestamps
                created = component.get('created')
                if created:
                    created_dt = created.dt
                    if isinstance(created_dt, datetime) and created_dt.tzinfo is not None:
                        event['created'] = created_dt.astimezone(local_tz)
                    else:
                        event['created'] = created_dt
                
                modified = component.get('last-modified')
                if modified:
                    modified_dt = modified.dt
                    if isinstance(modified_dt, datetime) and modified_dt.tzinfo is not None:
                        event['last_modified'] = modified_dt.astimezone(local_tz)
                    else:
                        event['last_modified'] = modified_dt
                
                events.append(event)
    
    return events


def get_calendar_week(dt: datetime) -> int:
    """Get ISO calendar week number"""
    return dt.isocalendar()[1]


# ============================================================================
# Calendar Importer
# ============================================================================

class CalendarImporter:
    """Import calendar events into ObjectStore + GraphLayer"""
    
    def __init__(self, obj_client: ObjectStoreClient, graph_client: GraphClient):
        self.obj = obj_client
        self.graph = graph_client
        self.year_nodes = {}
        self.month_nodes = {}
        self.week_nodes = {}
        self.day_nodes = {}
        self.event_nodes = {}
    
    def register_schemas(self):
        """Register all required schemas"""
        print("Registering schemas...")
        
        schemas = [
            {
                "class_name": "Year",
                "attributes": {
                    "year": "int",
                    "event_count": "int"
                },
                "description": "Calendar year"
            },
            {
                "class_name": "Month",
                "attributes": {
                    "year": "int",
                    "month": "int",
                    "month_name": "string",
                    "event_count": "int"
                },
                "description": "Calendar month"
            },
            {
                "class_name": "Week",
                "attributes": {
                    "year": "int",
                    "week": "int",
                    "start_date": "string",
                    "end_date": "string",
                    "event_count": "int"
                },
                "description": "Calendar week"
            },
            {
                "class_name": "Day",
                "attributes": {
                    "date": "string",
                    "year": "int",
                    "month": "int",
                    "day": "int",
                    "weekday": "string",
                    "event_count": "int"
                },
                "description": "Calendar day"
            },
            {
                "class_name": "Event",
                "attributes": {
                    "date": "string",
                    "start_time": "string",
                    "end_time": "string",
                    "duration_minutes": "int",
                    "summary": "string",
                    "status": "string",
                    "object_store_id": "string",
                    "object_url": "string"
                },
                "description": "Calendar event"
            }
        ]
        
        for schema in schemas:
            try:
                self.graph.register_schema(
                    class_name=schema["class_name"],
                    parent_class="Thing",
                    attributes=schema["attributes"],
                    description=schema["description"]
                )
                print(f"  ✓ {schema['class_name']} schema registered")
            except requests.HTTPError as e:
                if e.response.status_code == 400:
                    print(f"  ✓ {schema['class_name']} schema already exists")
                else:
                    raise
    
    def get_or_create_year_node(self, year: int) -> str:
        """Get or create year node"""
        if year in self.year_nodes:
            return self.year_nodes[year]
        
        node = self.graph.create_node(
            class_name="Year",
            name=str(year),
            attributes={
                "year": year,
                "event_count": 0
            }
        )
        node_id = node['node_id']
        self.year_nodes[year] = node_id
        return node_id
    
    def get_or_create_month_node(self, year: int, month: int) -> str:
        """Get or create month node"""
        key = (year, month)
        if key in self.month_nodes:
            return self.month_nodes[key]
        
        month_names = ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni',
                      'Juli', 'August', 'September', 'Oktober', 'November', 'Dezember']
        
        node = self.graph.create_node(
            class_name="Month",
            name=f"{year}-{month:02d} ({month_names[month-1]})",
            attributes={
                "year": year,
                "month": month,
                "month_name": month_names[month-1],
                "event_count": 0
            }
        )
        node_id = node['node_id']
        self.month_nodes[key] = node_id
        
        # Link to year
        year_node_id = self.get_or_create_year_node(year)
        try:
            edge = self.graph.create_edge(year_node_id, node_id, "contains_month")
            print(f"[DEBUG] Created edge: Year {year} -> Month {month} (edge_id: {edge.get('edge_id', 'N/A')})")
        except Exception as e:
            print(f"[ERROR] Failed to create edge Year -> Month: {e}")
            raise
        
        return node_id
    
    def get_or_create_week_node(self, year: int, week: int, 
                                start_date: datetime) -> str:
        """Get or create week node"""
        key = (year, week)
        if key in self.week_nodes:
            return self.week_nodes[key]
        
        # Calculate week start and end
        week_start = start_date - timedelta(days=start_date.weekday())
        week_end = week_start + timedelta(days=6)
        
        node = self.graph.create_node(
            class_name="Week",
            name=f"{year}-W{week:02d}",
            attributes={
                "year": year,
                "week": week,
                "start_date": week_start.strftime("%Y-%m-%d"),
                "end_date": week_end.strftime("%Y-%m-%d"),
                "event_count": 0
            }
        )
        node_id = node['node_id']
        self.week_nodes[key] = node_id
        
        # Link to year
        year_node_id = self.get_or_create_year_node(year)
        self.graph.create_edge(year_node_id, node_id, "contains_week")
        
        return node_id
    
    def get_or_create_day_node(self, dt: datetime) -> str:
        """Get or create day node"""
        date_str = dt.strftime("%Y-%m-%d")
        if date_str in self.day_nodes:
            return self.day_nodes[date_str]
        
        weekdays = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 
                   'Freitag', 'Samstag', 'Sonntag']
        
        node = self.graph.create_node(
            class_name="Day",
            name=date_str,
            attributes={
                "date": date_str,
                "year": dt.year,
                "month": dt.month,
                "day": dt.day,
                "weekday": weekdays[dt.weekday()],
                "event_count": 0
            }
        )
        node_id = node['node_id']
        self.day_nodes[date_str] = node_id
        
        # Link to month
        month_node_id = self.get_or_create_month_node(dt.year, dt.month)
        self.graph.create_edge(month_node_id, node_id, "contains_day")
        
        # Link to week
        week_num = get_calendar_week(dt)
        week_node_id = self.get_or_create_week_node(dt.year, week_num, dt)
        self.graph.create_edge(week_node_id, node_id, "contains_day")
        
        return node_id
    
    def import_events(self, events: List[Dict]):
        """Import all events"""
        print(f"\nImporting {len(events)} events...")
        
        for idx, event in enumerate(events, 1):
            try:
                self._import_event(event)
                print(f"  [{idx}/{len(events)}] ✓ {event['summary']}")
            except Exception as e:
                print(f"  [{idx}/{len(events)}] ✗ Failed: {e}")
        
        print(f"\nImport complete!")
        print(f"  Years: {len(self.year_nodes)}")
        print(f"  Months: {len(self.month_nodes)}")
        print(f"  Weeks: {len(self.week_nodes)}")
        print(f"  Days: {len(self.day_nodes)}")
        print(f"  Events: {len(self.event_nodes)}")
    
    def _import_event(self, event: Dict):
        """Import a single event"""
        start_dt = event['start']
        
        # Store event data in ObjectStore
        event_data = {
            "uid": event['uid'],
            "summary": event['summary'],
            "description": event.get('description', ''),
            "status": event.get('status', 'CONFIRMED'),
            "start": start_dt.isoformat(),
            "end": event['end'].isoformat(),
            "duration_minutes": event.get('duration_minutes', 0),
            "created": event.get('created').isoformat() if event.get('created') else None,
            "last_modified": event.get('last_modified').isoformat() if event.get('last_modified') else None
        }
        
        event_json = json.dumps(event_data, indent=2).encode('utf-8')
        obj_result = self.obj.store(
            event_json,
            content_type="application/json",
            metadata={
                "type": "calendar_event",
                "summary": event['summary'],
                "date": start_dt.strftime("%Y-%m-%d")
            }
        )
        obj_id = obj_result['object_id']
        obj_url = self.obj.get_object_url(obj_id)
        
        # Create event node in GraphLayer
        event_node = self.graph.create_node(
            class_name="Event",
            name=event['summary'],
            attributes={
                "date": start_dt.strftime("%Y-%m-%d"),
                "start_time": start_dt.strftime("%H:%M"),
                "end_time": event['end'].strftime("%H:%M"),
                "duration_minutes": event.get('duration_minutes', 0),
                "summary": event['summary'],
                "status": event.get('status', 'CONFIRMED'),
                "object_store_id": obj_id,
                "object_url": obj_url
            }
        )
        event_node_id = event_node['node_id']
        self.event_nodes[event['uid']] = event_node_id
        
        # Link to day
        day_node_id = self.get_or_create_day_node(start_dt)
        self.graph.create_edge(day_node_id, event_node_id, "has_event")


# ============================================================================
# Calendar Query Interface
# ============================================================================

class CalendarQuery:
    """Query calendar events"""
    
    def __init__(self, graph_client: GraphClient):
        self.graph = graph_client
    
    def query_by_date(self, date_str: str) -> List[Dict]:
        """Query events by specific date (YYYY-MM-DD)"""
        result = self.graph.filter_by_class("Day", {
            "filter": {
                "filters": [{"attribute": "date", "operator": "==", "value": date_str}],
                "logic": "AND"
            }
        })
        
        if result['count'] == 0:
            return []
        
        day_node = result['nodes'][0]
        events = self._get_day_events(day_node['node_id'])
        return events
    
    def query_by_month(self, year: int, month: int) -> Dict[str, List[Dict]]:
        """Query events by month"""
        result = self.graph.filter_by_class("Month", {
            "filter": {
                "filters": [
                    {"attribute": "year", "operator": "==", "value": str(year)},
                    {"attribute": "month", "operator": "==", "value": str(month)}
                ],
                "logic": "AND"
            }
        })
        
        if result['count'] == 0:
            return {}
        
        month_node_id = result['nodes'][0]['node_id']
        return self._get_month_events(month_node_id)
    
    def query_by_week(self, year: int, week: int) -> Dict[str, List[Dict]]:
        """Query events by calendar week"""
        result = self.graph.filter_by_class("Week", {
            "filter": {
                "filters": [
                    {"attribute": "year", "operator": "==", "value": str(year)},
                    {"attribute": "week", "operator": "==", "value": str(week)}
                ],
                "logic": "AND"
            }
        })
        
        if result['count'] == 0:
            return {}
        
        week_node_id = result['nodes'][0]['node_id']
        return self._get_week_events(week_node_id)
    
    def query_by_year(self, year: int) -> Dict[str, Dict[str, List[Dict]]]:
        """Query events by year"""
        result = self.graph.filter_by_class("Year", {
            "filter": {
                "filters": [{"attribute": "year", "operator": "==", "value": str(year)}],
                "logic": "AND"
            }
        })
        
        if result['count'] == 0:
            return {}
        
        year_node_id = result['nodes'][0]['node_id']
        return self._get_year_events(year_node_id)
    
    def query_all(self) -> Dict:
        """Query all events grouped by year"""
        result = self.graph.filter_by_class("Year", {})
        
        print(f"\n[DEBUG] Found {result['count']} year nodes")
        
        all_events = {}
        for year_node in result['nodes']:
            year = year_node['attributes']['year']
            print(f"[DEBUG] Processing year {year} (node_id: {year_node['node_id']})")
            year_events = self._get_year_events(year_node['node_id'])
            all_events[year] = year_events
            
            # Count total events for this year
            total_events = sum(len(events) for day_events in year_events.values() 
                             for events in day_events.values())
            print(f"[DEBUG] Year {year} has {total_events} events")
        
        return all_events
    
    def _get_day_events(self, day_node_id: str) -> List[Dict]:
        """Get all events for a day"""
        related = self.graph.get_related_nodes(day_node_id, direction="outgoing")
        
        print(f"[DEBUG] _get_day_events: day has {related['related_count']} related nodes")
        
        events = []
        for item in related['related']:
            print(f"[DEBUG] Day relationship: {item['relationship']}")
            if item['relationship'] == 'has_event':
                events.append(item['node'])
                print(f"[DEBUG] Found event: {item['node']['name']}")
        
        # Sort by start time
        events.sort(key=lambda e: e['attributes'].get('start_time', ''))
        return events
    
    def _get_week_events(self, week_node_id: str) -> Dict[str, List[Dict]]:
        """Get all events for a week, grouped by day"""
        related = self.graph.get_related_nodes(week_node_id, direction="outgoing")
        
        days_events = {}
        for item in related['related']:
            if item['relationship'] == 'contains_day':
                day_node = item['node']
                date = day_node['attributes']['date']
                days_events[date] = self._get_day_events(day_node['node_id'])
        
        # Sort by date
        return dict(sorted(days_events.items()))
    
    def _get_month_events(self, month_node_id: str) -> Dict[str, List[Dict]]:
        """Get all events for a month, grouped by day"""
        related = self.graph.get_related_nodes(month_node_id, direction="outgoing")
        
        print(f"[DEBUG] _get_month_events: month has {related['related_count']} related nodes")
        
        days_events = {}
        for item in related['related']:
            if item['relationship'] == 'contains_day':
                day_node = item['node']
                date = day_node['attributes']['date']
                day_events = self._get_day_events(day_node['node_id'])
                days_events[date] = day_events
                print(f"[DEBUG] Day {date} has {len(day_events)} events")
        
        # Sort by date
        return dict(sorted(days_events.items()))
    
    def _get_year_events(self, year_node_id: str) -> Dict[str, Dict[str, List[Dict]]]:
        """Get all events for a year, grouped by month and day"""
        related = self.graph.get_related_nodes(year_node_id, direction="outgoing")
        
        print(f"[DEBUG] _get_year_events: year node has {related['related_count']} related nodes")
        
        months_events = {}
        for item in related['related']:
            print(f"[DEBUG] Relationship: {item['relationship']}, Node: {item['node']['name']}")
            if item['relationship'] == 'contains_month':
                month_node = item['node']
                month_name = month_node['name']
                print(f"[DEBUG] Processing month: {month_name}")
                month_events = self._get_month_events(month_node['node_id'])
                months_events[month_name] = month_events
                
                # Count events in this month
                event_count = sum(len(events) for events in month_events.values())
                print(f"[DEBUG] Month {month_name} has {event_count} events")
        
        return months_events


# ============================================================================
# Display Functions
# ============================================================================

def print_separator(char="=", length=80):
    print(char * length)


def print_header(text: str):
    print(f"\n{text}")
    print_separator("-", len(text))


def display_events_by_day(events_by_day: Dict[str, List[Dict]]):
    """Display events grouped by day"""
    print(f"\n[DEBUG] display_events_by_day called with {len(events_by_day)} days")
    
    if not events_by_day:
        print("\n❌ Keine Termine gefunden.")
        return
    
    # Count total events
    total_events = sum(len(events) for events in events_by_day.values())
    print(f"[DEBUG] Total events across all days: {total_events}")
    
    if total_events == 0:
        print("\n❌ Keine Termine in den gefundenen Tagen.")
        return
    
    for date, events in events_by_day.items():
        if not events:
            print(f"\n[DEBUG] Skipping {date} - no events")
            continue
        
        # Parse date for nice display
        dt = datetime.strptime(date, "%Y-%m-%d")
        weekdays = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 
                   'Freitag', 'Samstag', 'Sonntag']
        weekday = weekdays[dt.weekday()]
        
        print(f"\n📅 {weekday}, {dt.strftime('%d.%m.%Y')}")
        print(f"   ({len(events)} Termin{'e' if len(events) > 1 else ''})")
        print_separator("-", 60)
        
        for event in events:
            attrs = event['attributes']
            start = attrs.get('start_time', 'N/A')
            end = attrs.get('end_time', 'N/A')
            duration = attrs.get('duration_minutes', 0)
            
            print(f"  ⏰ {start} - {end} ({duration} min)")
            print(f"     📝 {attrs.get('summary', 'Untitled')}")
            print(f"     🔗 {attrs.get('object_url', 'N/A')}")
            print()


# ============================================================================
# Main Program
# ============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Calendar Management System")
    parser.add_argument("--ical-file", required=True, help="Path to iCal file")
    parser.add_argument("--objectstore", default="http://localhost:5000", 
                       help="ObjectStore URL")
    parser.add_argument("--graphlayer", default="http://localhost:5001",
                       help="GraphLayer URL")
    parser.add_argument("--token", default="sk-admin-secret-token-123456",
                       help="GraphLayer API token")
    parser.add_argument("--import-only", action="store_true",
                       help="Only import, don't query")
    
    args = parser.parse_args()
    
    print_separator()
    print("Calendar Management System")
    print("iCal → ObjectStore + GraphLayer")
    print_separator()
    
    # Initialize clients
    obj_client = ObjectStoreClient(args.objectstore)
    graph_client = GraphClient(args.graphlayer, args.token)
    
    # Import calendar
    print_header("Phase 1: Import Calendar")
    
    importer = CalendarImporter(obj_client, graph_client)
    importer.register_schemas()
    
    print(f"\nParsing iCal file: {args.ical_file}")
    events = parse_ical_file(args.ical_file)
    print(f"Found {len(events)} events")
    
    importer.import_events(events)
    
    # Verification: Check what was created
    print_header("Import Verification")
    print(f"Created structure:")
    print(f"  Years: {len(importer.year_nodes)}")
    for year, node_id in sorted(importer.year_nodes.items()):
        print(f"    - {year}: {node_id}")
    print(f"  Months: {len(importer.month_nodes)}")
    print(f"  Weeks: {len(importer.week_nodes)}")
    print(f"  Days: {len(importer.day_nodes)}")
    print(f"  Events: {len(importer.event_nodes)}")
    
    if args.import_only:
        print("\nImport complete. Exiting.")
        return
    
    # Quick test: Try to query one year to verify data is accessible
    print_header("Testing Data Accessibility")
    try:
        if importer.year_nodes:
            test_year = list(importer.year_nodes.keys())[0]
            print(f"Testing query for year {test_year}...")
            query = CalendarQuery(graph_client)
            test_result = query.query_by_year(test_year)
            event_count = sum(
                len(events) 
                for month_events in test_result.values() 
                for events in month_events.values()
            )
            print(f"✓ Successfully queried year {test_year}: found {event_count} events")
        else:
            print("⚠️  No years imported, cannot test query")
    except Exception as e:
        print(f"✗ Test query failed: {e}")
        import traceback
        traceback.print_exc()
    
    # Interactive query
    print_header("Phase 2: Query Calendar")
    
    query = CalendarQuery(graph_client)
    
    while True:
        print("\n" + "="*80)
        print("Welche Termine möchten Sie anzeigen?")
        print("="*80)
        print("  1) Bestimmter Tag (YYYY-MM-DD)")
        print("  2) Bestimmte Woche (YYYY-WW)")
        print("  3) Bestimmter Monat (YYYY-MM)")
        print("  4) Bestimmtes Jahr (YYYY)")
        print("  5) Alle Termine")
        print("  0) Beenden")
        print("="*80)
        
        choice = input("\nIhre Wahl: ").strip()
        
        if choice == "0":
            print("\nAuf Wiedersehen!")
            break
        
        elif choice == "1":
            date_str = input("Datum (YYYY-MM-DD): ").strip()
            try:
                events = query.query_by_date(date_str)
                print_header(f"Termine am {date_str}")
                display_events_by_day({date_str: events})
            except Exception as e:
                print(f"Fehler: {e}")
        
        elif choice == "2":
            year_week = input("Jahr und Woche (YYYY-WW): ").strip()
            try:
                year, week = map(int, year_week.split('-'))
                events_by_day = query.query_by_week(year, week)
                print_header(f"Termine in Woche {week}/{year}")
                display_events_by_day(events_by_day)
            except Exception as e:
                print(f"Fehler: {e}")
        
        elif choice == "3":
            year_month = input("Jahr und Monat (YYYY-MM): ").strip()
            try:
                year, month = map(int, year_month.split('-'))
                events_by_day = query.query_by_month(year, month)
                print_header(f"Termine im Monat {month:02d}/{year}")
                display_events_by_day(events_by_day)
            except Exception as e:
                print(f"Fehler: {e}")
        
        elif choice == "4":
            year = input("Jahr (YYYY): ").strip()
            try:
                year = int(year)
                events_by_month = query.query_by_year(year)
                print_header(f"Termine im Jahr {year}")
                for month_name, events_by_day in events_by_month.items():
                    if any(events_by_day.values()):
                        print(f"\n{'='*80}")
                        print(f"  {month_name}")
                        print(f"{'='*80}")
                        display_events_by_day(events_by_day)
            except Exception as e:
                print(f"Fehler: {e}")
        
        elif choice == "5":
            try:
                print("\n[DEBUG] Querying all events...")
                all_events = query.query_all()
                print(f"[DEBUG] Got data for {len(all_events)} years")
                
                print_header("Alle Termine")
                
                if not all_events:
                    print("\n❌ Keine Jahre mit Terminen gefunden.")
                else:
                    for year in sorted(all_events.keys()):
                        events_by_month = all_events[year]
                        print(f"\n[DEBUG] Processing year {year} with {len(events_by_month)} months")
                        
                        # Check if this year has any events at all
                        year_has_events = False
                        for month_name, events_by_day in events_by_month.items():
                            if any(events_by_day.values()):
                                year_has_events = True
                                break
                        
                        if not year_has_events:
                            print(f"[DEBUG] Year {year} has no events, skipping")
                            continue
                        
                        print(f"\n{'#'*80}")
                        print(f"  Jahr {year}")
                        print(f"{'#'*80}")
                        
                        for month_name in sorted(events_by_month.keys()):
                            events_by_day = events_by_month[month_name]
                            
                            # Check if this month has any events
                            month_event_count = sum(len(events) for events in events_by_day.values())
                            print(f"[DEBUG] Month {month_name} has {month_event_count} events")
                            
                            if month_event_count > 0:
                                print(f"\n  {month_name}")
                                print(f"  {'-'*78}")
                                display_events_by_day(events_by_day)
            except Exception as e:
                print(f"Fehler: {e}")
                import traceback
                traceback.print_exc()
        
        else:
            print("Ungültige Eingabe. Bitte versuchen Sie es erneut.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nProgramm abgebrochen.")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ FEHLER: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
