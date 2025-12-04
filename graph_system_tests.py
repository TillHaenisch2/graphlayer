# ============================================================================
# File: test_graph_system.py
# Complete Unit Tests for Graph System with Class Hierarchies and Filters
# ============================================================================

import unittest
import tempfile
import shutil
from pathlib import Path
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_system import (
    NodeSchema, Node, Edge, SchemaRegistry, GraphStore, GraphQuery, EdgeType,
    AttributeFilter, FilterExpression, GraphFilter
)


class TestSchemaRegistry(unittest.TestCase):
    """Test Schema Registration and Class Hierarchy"""
    
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.registry = SchemaRegistry(self.temp_dir)
    
    def tearDown(self):
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_thing_schema_exists(self):
        schema = self.registry.get_schema("Thing")
        self.assertIsNotNone(schema)
        self.assertEqual(schema.class_name, "Thing")
        self.assertIsNone(schema.parent_class)
    
    def test_thing_has_base_attributes(self):
        schema = self.registry.get_schema("Thing")
        self.assertIn("name", schema.attributes)
        self.assertIn("type", schema.attributes)
        self.assertIn("timestamp", schema.attributes)
    
    def test_register_class(self):
        schema = self.registry.register_class(
            "Person",
            parent_class="Thing",
            attributes={"age": "int", "email": "string"},
            description="A person entity"
        )
        self.assertEqual(schema.class_name, "Person")
        self.assertEqual(schema.parent_class, "Thing")
    
    def test_register_duplicate_raises_error(self):
        self.registry.register_class("Person", parent_class="Thing")
        with self.assertRaises(ValueError):
            self.registry.register_class("Person", parent_class="Thing")
    
    def test_register_invalid_parent_raises_error(self):
        with self.assertRaises(ValueError):
            self.registry.register_class("Person", parent_class="InvalidParent")
    
    def test_class_hierarchy(self):
        self.registry.register_class("Animal", parent_class="Thing")
        self.registry.register_class("Dog", parent_class="Animal")
        hierarchy = self.registry.get_class_hierarchy("Dog")
        self.assertEqual(hierarchy, ["Dog", "Animal", "Thing"])
    
    def test_is_subclass_of(self):
        self.registry.register_class("Animal", parent_class="Thing")
        self.registry.register_class("Dog", parent_class="Animal")
        self.assertTrue(self.registry.is_subclass_of("Dog", "Animal"))
        self.assertTrue(self.registry.is_subclass_of("Dog", "Thing"))
        self.assertFalse(self.registry.is_subclass_of("Animal", "Dog"))
    
    def test_get_all_schemas(self):
        self.registry.register_class("Person", parent_class="Thing")
        self.registry.register_class("Organization", parent_class="Thing")
        schemas = self.registry.get_all_schemas()
        self.assertIn("Thing", schemas)
        self.assertIn("Person", schemas)
        self.assertIn("Organization", schemas)
    
    def test_schema_persistence(self):
        self.registry.register_class("Person", parent_class="Thing")
        registry2 = SchemaRegistry(self.temp_dir)
        schema = registry2.get_schema("Person")
        self.assertIsNotNone(schema)
        self.assertEqual(schema.class_name, "Person")


class TestGraphStore(unittest.TestCase):
    """Test Graph Store Operations"""
    
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.graph = GraphStore(self.temp_dir)
        self.graph.schema_registry.register_class("Person", parent_class="Thing", attributes={"age": "int"})
        self.graph.schema_registry.register_class("Organization", parent_class="Thing", attributes={"industry": "string"})
    
    def tearDown(self):
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_create_node(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30})
        self.assertIsNotNone(node.node_id)
        self.assertEqual(node.class_name, "Person")
        self.assertEqual(node.name, "Alice")
        self.assertEqual(node.attributes["age"], 30)
    
    def test_create_node_invalid_class(self):
        with self.assertRaises(ValueError):
            self.graph.create_node("InvalidClass", "test")
    
    def test_get_node(self):
        created = self.graph.create_node("Person", "Bob")
        retrieved = self.graph.get_node(created.node_id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.node_id, created.node_id)
    
    def test_get_nonexistent_node(self):
        result = self.graph.get_node("nonexistent")
        self.assertIsNone(result)
    
    def test_update_node(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30})
        updated = self.graph.update_node(node.node_id, name="Alice2", attributes={"age": 31})
        self.assertIsNotNone(updated)
        self.assertEqual(updated.name, "Alice2")
        self.assertEqual(updated.attributes["age"], 31)
    
    def test_update_nonexistent_node(self):
        result = self.graph.update_node("nonexistent")
        self.assertIsNone(result)
    
    def test_delete_node(self):
        node = self.graph.create_node("Person", "Alice")
        success = self.graph.delete_node(node.node_id)
        self.assertTrue(success)
        self.assertIsNone(self.graph.get_node(node.node_id))
    
    def test_delete_node_deletes_edges(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        edge = self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.delete_node(alice.node_id)
        self.assertIsNone(self.graph.get_edge(edge.edge_id))
    
    def test_create_edge(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        edge = self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.assertIsNotNone(edge.edge_id)
        self.assertEqual(edge.from_node_id, alice.node_id)
        self.assertEqual(edge.to_node_id, bob.node_id)
    
    def test_create_edge_invalid_source(self):
        bob = self.graph.create_node("Person", "Bob")
        with self.assertRaises(ValueError):
            self.graph.create_edge("nonexistent", bob.node_id, "knows")
    
    def test_create_edge_invalid_target(self):
        alice = self.graph.create_node("Person", "Alice")
        with self.assertRaises(ValueError):
            self.graph.create_edge(alice.node_id, "nonexistent", "knows")
    
    def test_get_edge(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        created = self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        retrieved = self.graph.get_edge(created.edge_id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.edge_id, created.edge_id)
    
    def test_delete_edge(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        edge = self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        success = self.graph.delete_edge(edge.edge_id)
        self.assertTrue(success)
        self.assertIsNone(self.graph.get_edge(edge.edge_id))
    
    def test_get_outgoing_edges(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        charlie = self.graph.create_node("Person", "Charlie")
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(alice.node_id, charlie.node_id, "knows")
        outgoing = self.graph.get_outgoing_edges(alice.node_id)
        self.assertEqual(len(outgoing), 2)
    
    def test_get_outgoing_edges_by_type(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        company = self.graph.create_node("Organization", "ACME")
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(alice.node_id, company.node_id, "works_at")
        knows_edges = self.graph.get_outgoing_edges(alice.node_id, "knows")
        self.assertEqual(len(knows_edges), 1)
        self.assertEqual(knows_edges[0].edge_type, "knows")
    
    def test_get_incoming_edges(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        charlie = self.graph.create_node("Person", "Charlie")
        self.graph.create_edge(bob.node_id, alice.node_id, "knows")
        self.graph.create_edge(charlie.node_id, alice.node_id, "knows")
        incoming = self.graph.get_incoming_edges(alice.node_id)
        self.assertEqual(len(incoming), 2)
    
    def test_find_nodes_by_class(self):
        self.graph.create_node("Person", "Alice")
        self.graph.create_node("Person", "Bob")
        self.graph.create_node("Organization", "ACME")
        persons = self.graph.find_nodes_by_class("Person")
        self.assertEqual(len(persons), 2)
    
    def test_find_nodes_by_name(self):
        self.graph.create_node("Person", "Alice")
        self.graph.create_node("Person", "Alice")
        self.graph.create_node("Organization", "Alice")
        nodes = self.graph.find_nodes_by_name("Alice")
        self.assertEqual(len(nodes), 3)
    
    def test_graph_persistence(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        graph2 = GraphStore(self.temp_dir)
        retrieved_alice = graph2.get_node(alice.node_id)
        self.assertIsNotNone(retrieved_alice)
        self.assertEqual(retrieved_alice.name, "Alice")


class TestAttributeFilter(unittest.TestCase):
    """Test Attribute Filtering"""
    
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.graph = GraphStore(self.temp_dir)
        self.graph.schema_registry.register_class("Person", parent_class="Thing", attributes={"age": "int", "city": "string"})
    
    def tearDown(self):
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_filter_equality(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30})
        filter_eq = AttributeFilter("age", "==", 30)
        self.assertTrue(filter_eq.matches(node))
        filter_ne = AttributeFilter("age", "==", 25)
        self.assertFalse(filter_ne.matches(node))
    
    def test_filter_not_equal(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30})
        filter_ne = AttributeFilter("age", "!=", 25)
        self.assertTrue(filter_ne.matches(node))
        filter_eq = AttributeFilter("age", "!=", 30)
        self.assertFalse(filter_eq.matches(node))
    
    def test_filter_less_equal(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30})
        filter_le = AttributeFilter("age", "<=", 30)
        self.assertTrue(filter_le.matches(node))
        filter_le2 = AttributeFilter("age", "<=", 25)
        self.assertFalse(filter_le2.matches(node))
    
    def test_filter_greater_equal(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30})
        filter_ge = AttributeFilter("age", ">=", 30)
        self.assertTrue(filter_ge.matches(node))
        filter_ge2 = AttributeFilter("age", ">=", 35)
        self.assertFalse(filter_ge2.matches(node))
    
    def test_filter_less_than(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30})
        filter_lt = AttributeFilter("age", "<", 35)
        self.assertTrue(filter_lt.matches(node))
        filter_lt2 = AttributeFilter("age", "<", 30)
        self.assertFalse(filter_lt2.matches(node))
    
    def test_filter_greater_than(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30})
        filter_gt = AttributeFilter("age", ">", 25)
        self.assertTrue(filter_gt.matches(node))
        filter_gt2 = AttributeFilter("age", ">", 30)
        self.assertFalse(filter_gt2.matches(node))
    
    def test_filter_in(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30})
        filter_in = AttributeFilter("age", "in", [25, 30, 35])
        self.assertTrue(filter_in.matches(node))
        filter_in2 = AttributeFilter("age", "in", [20, 25, 35])
        self.assertFalse(filter_in2.matches(node))
    
    def test_filter_contains(self):
        node = self.graph.create_node("Person", "Alice", {"city": "New York"})
        filter_contains = AttributeFilter("city", "contains", "York")
        self.assertTrue(filter_contains.matches(node))
        filter_contains2 = AttributeFilter("city", "contains", "London")
        self.assertFalse(filter_contains2.matches(node))
    
    def test_invalid_operator_raises_error(self):
        with self.assertRaises(ValueError):
            AttributeFilter("age", "invalid_op", 30)


class TestFilterExpression(unittest.TestCase):
    """Test Filter Expressions with AND/OR logic"""
    
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.graph = GraphStore(self.temp_dir)
        self.graph.schema_registry.register_class("Person", parent_class="Thing", attributes={"age": "int", "city": "string"})
    
    def tearDown(self):
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_and_expression(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30, "city": "NYC"})
        expr = FilterExpression([AttributeFilter("age", ">=", 25), AttributeFilter("city", "==", "NYC")], operator="AND")
        self.assertTrue(expr.matches(node))
        expr2 = FilterExpression([AttributeFilter("age", ">=", 25), AttributeFilter("city", "==", "LA")], operator="AND")
        self.assertFalse(expr2.matches(node))
    
    def test_or_expression(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30, "city": "NYC"})
        expr = FilterExpression([AttributeFilter("age", "==", 25), AttributeFilter("city", "==", "NYC")], operator="OR")
        self.assertTrue(expr.matches(node))
        expr2 = FilterExpression([AttributeFilter("age", "==", 25), AttributeFilter("city", "==", "LA")], operator="OR")
        self.assertFalse(expr2.matches(node))
    
    def test_add_filter(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30, "city": "NYC"})
        expr = FilterExpression(operator="AND")
        expr.add_filter(AttributeFilter("age", ">=", 25))
        expr.add_filter(AttributeFilter("city", "==", "NYC"))
        self.assertTrue(expr.matches(node))
    
    def test_empty_expression_matches_all(self):
        node = self.graph.create_node("Person", "Alice", {"age": 30})
        expr = FilterExpression()
        self.assertTrue(expr.matches(node))


class TestGraphFilter(unittest.TestCase):
    """Test Graph Filtering Engine"""
    
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.graph = GraphStore(self.temp_dir)
        self.graph_filter = GraphFilter(self.graph)
        self.graph.schema_registry.register_class("Person", parent_class="Thing", attributes={"age": "int", "city": "string"})
    
    def tearDown(self):
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_connected_nodes_excluding_is_a(self):
        alice = self.graph.create_node("Person", "Alice", {"age": 30})
        bob = self.graph.create_node("Person", "Bob", {"age": 25})
        charlie = self.graph.create_node("Person", "Charlie", {"age": 35})
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(alice.node_id, charlie.node_id, "is_a")
        connected = self.graph_filter.get_connected_nodes_excluding_is_a(alice.node_id)
        self.assertEqual(len(connected), 1)
        self.assertEqual(connected[0].name, "Bob")
    
    def test_connected_nodes_with_attribute_filter(self):
        alice = self.graph.create_node("Person", "Alice", {"age": 30})
        bob = self.graph.create_node("Person", "Bob", {"age": 25})
        charlie = self.graph.create_node("Person", "Charlie", {"age": 35})
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(alice.node_id, charlie.node_id, "knows")
        filter_expr = FilterExpression([AttributeFilter("age", ">=", 30)])
        connected = self.graph_filter.get_connected_nodes_excluding_is_a(alice.node_id, attribute_filter=filter_expr)
        self.assertEqual(len(connected), 1)
        self.assertEqual(connected[0].name, "Charlie")
    
    def test_traverse_with_filter(self):
        alice = self.graph.create_node("Person", "Alice", {"age": 30})
        bob = self.graph.create_node("Person", "Bob", {"age": 25})
        charlie = self.graph.create_node("Person", "Charlie", {"age": 35})
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(bob.node_id, charlie.node_id, "knows")
        filter_expr = FilterExpression([AttributeFilter("age", ">=", 30)])
        traversed = self.graph_filter.traverse_with_filter(alice.node_id, exclude_edge_types=["is_a"], attribute_filter=filter_expr, max_depth=5)
        self.assertEqual(len(traversed), 2)
        names = {n.name for n in traversed}
        self.assertIn("Alice", names)
        self.assertIn("Charlie", names)
        self.assertNotIn("Bob", names)
    
    def test_filter_nodes_by_class(self):
        self.graph.create_node("Person", "Alice", {"age": 30})
        self.graph.create_node("Person", "Bob", {"age": 25})
        self.graph.create_node("Person", "Charlie", {"age": 35})
        filter_expr = FilterExpression([AttributeFilter("age", ">=", 30)])
        nodes = self.graph_filter.filter_nodes_by_class("Person", filter_expr)
        self.assertEqual(len(nodes), 2)
    
    def test_filter_all_nodes(self):
        self.graph.create_node("Person", "Alice", {"age": 30})
        self.graph.create_node("Person", "Bob", {"age": 25})
        self.graph.create_node("Person", "Charlie", {"age": 35})
        filter_expr = FilterExpression([AttributeFilter("age", ">", 25)])
        nodes = self.graph_filter.filter_all_nodes(filter_expr)
        self.assertEqual(len(nodes), 2)
    
    def test_connected_nodes_with_complex_filter(self):
        alice = self.graph.create_node("Person", "Alice", {"age": 30, "city": "NYC"})
        bob = self.graph.create_node("Person", "Bob", {"age": 25, "city": "NYC"})
        charlie = self.graph.create_node("Person", "Charlie", {"age": 35, "city": "LA"})
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(alice.node_id, charlie.node_id, "knows")
        filter_expr = FilterExpression([AttributeFilter("age", ">=", 30), AttributeFilter("city", "in", ["NYC", "LA"])], operator="AND")
        connected = self.graph_filter.get_connected_nodes_excluding_is_a(alice.node_id, attribute_filter=filter_expr)
        self.assertEqual(len(connected), 1)
        self.assertEqual(connected[0].name, "Charlie")
    
    def test_traverse_with_depth_limit(self):
        n1 = self.graph.create_node("Person", "N1", {"age": 20})
        n2 = self.graph.create_node("Person", "N2", {"age": 20})
        n3 = self.graph.create_node("Person", "N3", {"age": 20})
        n4 = self.graph.create_node("Person", "N4", {"age": 20})
        self.graph.create_edge(n1.node_id, n2.node_id, "knows")
        self.graph.create_edge(n2.node_id, n3.node_id, "knows")
        self.graph.create_edge(n3.node_id, n4.node_id, "knows")
        traversed = self.graph_filter.traverse_with_filter(n1.node_id, exclude_edge_types=["is_a"], max_depth=1)
        self.assertEqual(len(traversed), 2)
        names = {n.name for n in traversed}
        self.assertIn("N1", names)
        self.assertIn("N2", names)


class TestGraphQuery(unittest.TestCase):
    """Test Graph Query Engine"""
    
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.graph = GraphStore(self.temp_dir)
        self.graph.schema_registry.register_class("Person", parent_class="Thing", attributes={"age": "int"})
        self.query = GraphQuery(self.graph)
    
    def tearDown(self):
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_find_direct_path(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        paths = self.query.find_paths(alice.node_id, bob.node_id)
        self.assertEqual(len(paths), 1)
        self.assertEqual(paths[0], [alice.node_id, bob.node_id])
    
    def test_find_multiple_paths(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        charlie = self.graph.create_node("Person", "Charlie")
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(alice.node_id, charlie.node_id, "knows")
        self.graph.create_edge(charlie.node_id, bob.node_id, "knows")
        paths = self.query.find_paths(alice.node_id, bob.node_id)
        self.assertGreaterEqual(len(paths), 1)
    
    def test_find_no_path(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        paths = self.query.find_paths(alice.node_id, bob.node_id)
        self.assertEqual(len(paths), 0)
    
    def test_find_related_nodes_outgoing(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        charlie = self.graph.create_node("Person", "Charlie")
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(alice.node_id, charlie.node_id, "knows")
        related = self.query.find_related_nodes(alice.node_id, direction="out")
        self.assertEqual(len(related), 2)
    
    def test_find_related_nodes_incoming(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        charlie = self.graph.create_node("Person", "Charlie")
        self.graph.create_edge(bob.node_id, alice.node_id, "knows")
        self.graph.create_edge(charlie.node_id, alice.node_id, "knows")
        related = self.query.find_related_nodes(alice.node_id, direction="in")
        self.assertEqual(len(related), 2)
    
    def test_find_related_nodes_both(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        charlie = self.graph.create_node("Person", "Charlie")
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(charlie.node_id, alice.node_id, "knows")
        related = self.query.find_related_nodes(alice.node_id, direction="both")
        self.assertEqual(len(related), 2)
    
    def test_find_related_by_type(self):
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        charlie = self.graph.create_node("Person", "Charlie")
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(alice.node_id, charlie.node_id, "works_with")
        knows_related = self.query.find_related_nodes(alice.node_id, relationship_type="knows")
        self.assertEqual(len(knows_related), 1)
        self.assertEqual(knows_related[0][1], "knows")
    
    def test_get_nodes_by_class_and_attribute(self):
        self.graph.create_node("Person", "Alice", {"age": 30})
        self.graph.create_node("Person", "Bob", {"age": 30})
        self.graph.create_node("Person", "Charlie", {"age": 25})
        nodes = self.query.get_nodes_by_class_and_attribute("Person", "age", 30)
        self.assertEqual(len(nodes), 2)


if __name__ == "__main__":
    unittest.main()
