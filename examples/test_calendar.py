#!/usr/bin/env python3
"""
Unit Tests for Calendar Management System
Tests import, storage, and query functionality
"""

import unittest
import json
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import sys

# Mock the clients to avoid needing actual servers
class MockObjectStoreClient:
    def __init__(self, base_url="http://localhost:5000", api_token=""):
        self.base_url = base_url
        self.stored_objects = {}
        self.next_id = 1
    
    def store(self, data, content_type="application/json", metadata=None):
        obj_id = f"test-obj-{self.next_id:04d}"
        self.next_id += 1
        self.stored_objects[obj_id] = {
            'data': data,
            'content_type': content_type,
            'metadata': metadata
        }
        return {
            'object_id': obj_id,
            'size': len(data),
            'version': 0
        }
    
    def get_object_url(self, object_id):
        return f"{self.base_url}/api/v1/objects/{object_id}"


class MockGraphClient:
    def __init__(self, base_url="http://localhost:5001", token=""):
        self.base_url = base_url
        self.nodes = {}
        self.edges = {}
        self.schemas = {}
        self.next_node_id = 1
        self.next_edge_id = 1
    
    def register_schema(self, class_name, parent_class="Thing", attributes=None, description=""):
        if class_name in self.schemas:
            raise Exception("Schema already exists")
        self.schemas[class_name] = {
            'class_name': class_name,
            'parent_class': parent_class,
            'attributes': attributes or {},
            'description': description
        }
        return self.schemas[class_name]
    
    def create_node(self, class_name, name, attributes=None):
        node_id = f"{class_name}:{self.next_node_id:04d}"
        self.next_node_id += 1
        
        node = {
            'node_id': node_id,
            'class_name': class_name,
            'name': name,
            'attributes': attributes or {},
            'created_at': datetime.now().isoformat()
        }
        self.nodes[node_id] = node
        return node
    
    def create_edge(self, from_node_id, to_node_id, edge_type="related_to", attributes=None):
        edge_id = f"edge-{self.next_edge_id:04d}"
        self.next_edge_id += 1
        
        edge = {
            'edge_id': edge_id,
            'from_node_id': from_node_id,
            'to_node_id': to_node_id,
            'edge_type': edge_type,
            'attributes': attributes or {}
        }
        self.edges[edge_id] = edge
        return edge
    
    def get_related_nodes(self, node_id, direction="outgoing"):
        related = []
        
        if direction in ("outgoing", "both"):
            for edge in self.edges.values():
                if edge['from_node_id'] == node_id:
                    to_node = self.nodes.get(edge['to_node_id'])
                    if to_node:
                        related.append({
                            'node': to_node,
                            'relationship': edge['edge_type']
                        })
        
        if direction in ("incoming", "both"):
            for edge in self.edges.values():
                if edge['to_node_id'] == node_id:
                    from_node = self.nodes.get(edge['from_node_id'])
                    if from_node:
                        related.append({
                            'node': from_node,
                            'relationship': edge['edge_type']
                        })
        
        return {
            'node_id': node_id,
            'direction': direction,
            'related_count': len(related),
            'related': related
        }
    
    def filter_by_class(self, class_name, filters=None):
        matching_nodes = [
            node for node in self.nodes.values() 
            if node['class_name'] == class_name
        ]
        
        # Apply filters if provided
        if filters and 'filter' in filters:
            filter_data = filters['filter']
            if 'filters' in filter_data:
                for f in filter_data['filters']:
                    attr = f['attribute']
                    op = f['operator']
                    value = f['value']
                    
                    filtered = []
                    for node in matching_nodes:
                        node_value = str(node['attributes'].get(attr, ''))
                        if op == "==" and node_value == str(value):
                            filtered.append(node)
                    matching_nodes = filtered
        
        return {
            'class_name': class_name,
            'count': len(matching_nodes),
            'nodes': matching_nodes
        }


# ============================================================================
# Unit Tests
# ============================================================================

class TestCalendarImport(unittest.TestCase):
    """Test calendar import functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        from calendar_manager import CalendarImporter
        
        self.obj_client = MockObjectStoreClient()
        self.graph_client = MockGraphClient()
        self.importer = CalendarImporter(self.obj_client, self.graph_client)
    
    def test_schema_registration(self):
        """Test that all required schemas are registered"""
        self.importer.register_schemas()
        
        required_schemas = ['Year', 'Month', 'Week', 'Day', 'Event']
        for schema_name in required_schemas:
            self.assertIn(schema_name, self.graph_client.schemas,
                         f"Schema {schema_name} should be registered")
    
    def test_year_node_creation(self):
        """Test year node creation"""
        self.importer.register_schemas()
        
        year_node_id = self.importer.get_or_create_year_node(2026)
        
        # Verify node was created
        self.assertIn(year_node_id, self.graph_client.nodes)
        node = self.graph_client.nodes[year_node_id]
        self.assertEqual(node['class_name'], 'Year')
        self.assertEqual(node['attributes']['year'], 2026)
        
        # Verify it's cached
        self.assertIn(2026, self.importer.year_nodes)
        self.assertEqual(self.importer.year_nodes[2026], year_node_id)
    
    def test_year_node_not_duplicated(self):
        """Test that year nodes are not duplicated"""
        self.importer.register_schemas()
        
        year_node_id_1 = self.importer.get_or_create_year_node(2026)
        year_node_id_2 = self.importer.get_or_create_year_node(2026)
        
        # Should return the same node ID
        self.assertEqual(year_node_id_1, year_node_id_2)
        
        # Should only have one year node
        year_nodes = [n for n in self.graph_client.nodes.values() 
                     if n['class_name'] == 'Year' and n['attributes']['year'] == 2026]
        self.assertEqual(len(year_nodes), 1, "Should only create one Year node per year")
    
    def test_month_node_creation(self):
        """Test month node creation"""
        self.importer.register_schemas()
        
        month_node_id = self.importer.get_or_create_month_node(2026, 1)
        
        # Verify node was created
        self.assertIn(month_node_id, self.graph_client.nodes)
        node = self.graph_client.nodes[month_node_id]
        self.assertEqual(node['class_name'], 'Month')
        self.assertEqual(node['attributes']['year'], 2026)
        self.assertEqual(node['attributes']['month'], 1)
    
    def test_month_linked_to_year(self):
        """Test that month is linked to year"""
        self.importer.register_schemas()
        
        year_node_id = self.importer.get_or_create_year_node(2026)
        month_node_id = self.importer.get_or_create_month_node(2026, 1)
        
        # Check that edge exists
        edge_found = False
        for edge in self.graph_client.edges.values():
            if (edge['from_node_id'] == year_node_id and 
                edge['to_node_id'] == month_node_id and
                edge['edge_type'] == 'contains_month'):
                edge_found = True
                break
        
        self.assertTrue(edge_found, "Year should be linked to Month with 'contains_month' edge")
    
    def test_day_node_creation(self):
        """Test day node creation"""
        self.importer.register_schemas()
        
        test_date = datetime(2026, 1, 15)
        day_node_id = self.importer.get_or_create_day_node(test_date)
        
        # Verify node was created
        self.assertIn(day_node_id, self.graph_client.nodes)
        node = self.graph_client.nodes[day_node_id]
        self.assertEqual(node['class_name'], 'Day')
        self.assertEqual(node['attributes']['date'], '2026-01-15')
        self.assertEqual(node['attributes']['year'], 2026)
        self.assertEqual(node['attributes']['month'], 1)
        self.assertEqual(node['attributes']['day'], 15)
    
    def test_day_linked_to_month(self):
        """Test that day is linked to month"""
        self.importer.register_schemas()
        
        test_date = datetime(2026, 1, 15)
        day_node_id = self.importer.get_or_create_day_node(test_date)
        month_node_id = self.importer.month_nodes[(2026, 1)]
        
        # Check that edge exists
        edge_found = False
        for edge in self.graph_client.edges.values():
            if (edge['from_node_id'] == month_node_id and 
                edge['to_node_id'] == day_node_id and
                edge['edge_type'] == 'contains_day'):
                edge_found = True
                break
        
        self.assertTrue(edge_found, "Month should be linked to Day with 'contains_day' edge")
    
    def test_event_import(self):
        """Test single event import"""
        self.importer.register_schemas()
        
        test_event = {
            'uid': 'test-event-001',
            'summary': 'Test Meeting',
            'description': 'A test meeting',
            'status': 'CONFIRMED',
            'start': datetime(2026, 1, 15, 10, 0),
            'end': datetime(2026, 1, 15, 11, 30),
            'duration_minutes': 90
        }
        
        self.importer._import_event(test_event)
        
        # Verify object was stored
        self.assertGreater(len(self.obj_client.stored_objects), 0)
        
        # Verify event node was created
        event_nodes = [n for n in self.graph_client.nodes.values() 
                      if n['class_name'] == 'Event']
        self.assertEqual(len(event_nodes), 1)
        
        event_node = event_nodes[0]
        self.assertEqual(event_node['attributes']['summary'], 'Test Meeting')
        self.assertEqual(event_node['attributes']['duration_minutes'], 90)
    
    def test_event_linked_to_day(self):
        """Test that event is linked to day"""
        self.importer.register_schemas()
        
        test_event = {
            'uid': 'test-event-001',
            'summary': 'Test Meeting',
            'description': '',
            'status': 'CONFIRMED',
            'start': datetime(2026, 1, 15, 10, 0),
            'end': datetime(2026, 1, 15, 11, 30),
            'duration_minutes': 90
        }
        
        self.importer._import_event(test_event)
        
        day_node_id = self.importer.day_nodes['2026-01-15']
        event_node = [n for n in self.graph_client.nodes.values() 
                     if n['class_name'] == 'Event'][0]
        
        # Check that edge exists
        edge_found = False
        for edge in self.graph_client.edges.values():
            if (edge['from_node_id'] == day_node_id and 
                edge['to_node_id'] == event_node['node_id'] and
                edge['edge_type'] == 'has_event'):
                edge_found = True
                break
        
        self.assertTrue(edge_found, "Day should be linked to Event with 'has_event' edge")
    
    def test_full_hierarchy(self):
        """Test complete hierarchy: Year -> Month -> Day -> Event"""
        self.importer.register_schemas()
        
        test_event = {
            'uid': 'test-event-001',
            'summary': 'Test Meeting',
            'description': '',
            'status': 'CONFIRMED',
            'start': datetime(2026, 1, 15, 10, 0),
            'end': datetime(2026, 1, 15, 11, 30),
            'duration_minutes': 90
        }
        
        self.importer._import_event(test_event)
        
        # Verify hierarchy exists
        year_node_id = self.importer.year_nodes[2026]
        month_node_id = self.importer.month_nodes[(2026, 1)]
        day_node_id = self.importer.day_nodes['2026-01-15']
        event_node_id = self.importer.event_nodes['test-event-001']
        
        # Verify Year -> Month link
        year_related = self.graph_client.get_related_nodes(year_node_id, "outgoing")
        month_ids = [r['node']['node_id'] for r in year_related['related'] 
                    if r['relationship'] == 'contains_month']
        self.assertIn(month_node_id, month_ids, "Year should link to Month")
        
        # Verify Month -> Day link
        month_related = self.graph_client.get_related_nodes(month_node_id, "outgoing")
        day_ids = [r['node']['node_id'] for r in month_related['related'] 
                  if r['relationship'] == 'contains_day']
        self.assertIn(day_node_id, day_ids, "Month should link to Day")
        
        # Verify Day -> Event link
        day_related = self.graph_client.get_related_nodes(day_node_id, "outgoing")
        event_ids = [r['node']['node_id'] for r in day_related['related'] 
                    if r['relationship'] == 'has_event']
        self.assertIn(event_node_id, event_ids, "Day should link to Event")


class TestCalendarQuery(unittest.TestCase):
    """Test calendar query functionality"""
    
    def setUp(self):
        """Set up test fixtures with sample data"""
        from calendar_manager import CalendarImporter, CalendarQuery
        
        self.obj_client = MockObjectStoreClient()
        self.graph_client = MockGraphClient()
        self.importer = CalendarImporter(self.obj_client, self.graph_client)
        self.query = CalendarQuery(self.graph_client)
        
        # Set up test data
        self.importer.register_schemas()
        
        # Create test events
        self.test_events = [
            {
                'uid': 'event-001',
                'summary': 'Morning Meeting',
                'description': '',
                'status': 'CONFIRMED',
                'start': datetime(2026, 1, 15, 9, 0),
                'end': datetime(2026, 1, 15, 10, 0),
                'duration_minutes': 60
            },
            {
                'uid': 'event-002',
                'summary': 'Afternoon Workshop',
                'description': '',
                'status': 'CONFIRMED',
                'start': datetime(2026, 1, 15, 14, 0),
                'end': datetime(2026, 1, 15, 17, 0),
                'duration_minutes': 180
            },
            {
                'uid': 'event-003',
                'summary': 'Project Review',
                'description': '',
                'status': 'CONFIRMED',
                'start': datetime(2026, 1, 16, 10, 0),
                'end': datetime(2026, 1, 16, 11, 30),
                'duration_minutes': 90
            }
        ]
        
        for event in self.test_events:
            self.importer._import_event(event)
    
    def test_query_by_date(self):
        """Test querying events by specific date"""
        events = self.query.query_by_date('2026-01-15')
        
        self.assertEqual(len(events), 2, "Should find 2 events on 2026-01-15")
        
        summaries = [e['attributes']['summary'] for e in events]
        self.assertIn('Morning Meeting', summaries)
        self.assertIn('Afternoon Workshop', summaries)
    
    def test_query_by_date_no_events(self):
        """Test querying date with no events"""
        events = self.query.query_by_date('2026-01-20')
        
        self.assertEqual(len(events), 0, "Should find no events on 2026-01-20")
    
    def test_query_by_month(self):
        """Test querying events by month"""
        events_by_day = self.query.query_by_month(2026, 1)
        
        # Should have 2 days with events
        self.assertEqual(len(events_by_day), 2)
        self.assertIn('2026-01-15', events_by_day)
        self.assertIn('2026-01-16', events_by_day)
        
        # Check event counts
        self.assertEqual(len(events_by_day['2026-01-15']), 2)
        self.assertEqual(len(events_by_day['2026-01-16']), 1)
    
    def test_query_by_year(self):
        """Test querying events by year"""
        events_by_month = self.query.query_by_year(2026)
        
        # Should have data
        self.assertGreater(len(events_by_month), 0, "Should have at least one month")
        
        # Find the January month
        january_found = False
        for month_name, events_by_day in events_by_month.items():
            if 'Januar' in month_name or '2026-01' in month_name:
                january_found = True
                # Should have 2 days with events
                self.assertEqual(len(events_by_day), 2)
                break
        
        self.assertTrue(january_found, "Should find January in year query")
    
    def test_events_sorted_by_time(self):
        """Test that events are sorted by start time"""
        events = self.query.query_by_date('2026-01-15')
        
        # Check that events are in chronological order
        start_times = [e['attributes']['start_time'] for e in events]
        self.assertEqual(start_times, sorted(start_times), 
                        "Events should be sorted by start time")


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error handling"""
    
    def test_multiple_years(self):
        """Test importing events across multiple years"""
        from calendar_manager import CalendarImporter
        
        obj_client = MockObjectStoreClient()
        graph_client = MockGraphClient()
        importer = CalendarImporter(obj_client, graph_client)
        importer.register_schemas()
        
        events = [
            {
                'uid': 'event-2025',
                'summary': '2025 Event',
                'description': '',
                'status': 'CONFIRMED',
                'start': datetime(2025, 12, 31, 23, 0),
                'end': datetime(2026, 1, 1, 1, 0),
                'duration_minutes': 120
            },
            {
                'uid': 'event-2026',
                'summary': '2026 Event',
                'description': '',
                'status': 'CONFIRMED',
                'start': datetime(2026, 1, 1, 10, 0),
                'end': datetime(2026, 1, 1, 11, 0),
                'duration_minutes': 60
            }
        ]
        
        for event in events:
            importer._import_event(event)
        
        # Should have created 2 year nodes
        self.assertEqual(len(importer.year_nodes), 2)
        self.assertIn(2025, importer.year_nodes)
        self.assertIn(2026, importer.year_nodes)


def run_tests():
    """Run all tests"""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestCalendarImport))
    suite.addTests(loader.loadTestsFromTestCase(TestCalendarQuery))
    suite.addTests(loader.loadTestsFromTestCase(TestEdgeCases))
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("="*80)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
