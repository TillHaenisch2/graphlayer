# ============================================================================
# File: test_graph_api.py
# Complete Unit Tests for Graph REST API
# ============================================================================

import unittest
import json
import tempfile
import shutil
from pathlib import Path
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_system import GraphStore
from graph_api import GraphAPI, APITokenManager


class TestGraphAPI(unittest.TestCase):
    """Test Graph REST API"""
    
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.graph = GraphStore(self.temp_dir)
        self.tokens = APITokenManager({"test_user": "test_token_123"})
        self.api = GraphAPI(self.graph, self.tokens)
        self.client = self.api.app.test_client()
        
        # Register test schema
        self.graph.schema_registry.register_class(
            "Person",
            parent_class="Thing",
            attributes={"age": "int", "city": "string"}
        )
    
    def tearDown(self):
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def _get_auth_header(self):
        """Get authorization header"""
        return {"Authorization": "Bearer test_token_123"}
    
    # ===================== Health & Info Tests =====================
    
    def test_health_check(self):
        """Test health check endpoint"""
        response = self.client.get("/api/v1/health")
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["status"], "ok")
        self.assertIn("timestamp", data)
    
    def test_get_stats(self):
        """Test getting statistics"""
        response = self.client.get("/api/v1/stats", headers=self._get_auth_header())
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn("total_nodes", data)
        self.assertIn("total_edges", data)
    
    def test_stats_requires_auth(self):
        """Test that stats endpoint requires auth"""
        response = self.client.get("/api/v1/stats")
        self.assertEqual(response.status_code, 401)
    
    # ===================== Schema Routes Tests =====================
    
    def test_get_all_schemas(self):
        """Test getting all schemas"""
        response = self.client.get("/api/v1/schemas", headers=self._get_auth_header())
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn("Thing", data["schemas"])
        self.assertIn("Person", data["schemas"])
    
    def test_get_specific_schema(self):
        """Test getting specific schema"""
        response = self.client.get("/api/v1/schemas/Person", headers=self._get_auth_header())
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["class_name"], "Person")
        self.assertEqual(data["parent_class"], "Thing")
    
    def test_get_nonexistent_schema(self):
        """Test getting nonexistent schema"""
        response = self.client.get("/api/v1/schemas/NonExistent", headers=self._get_auth_header())
        self.assertEqual(response.status_code, 404)
    
    def test_register_schema(self):
        """Test registering new schema"""
        payload = {
            "class_name": "Company",
            "parent_class": "Thing",
            "attributes": {"industry": "string", "employees": "int"},
            "description": "A company entity"
        }
        response = self.client.post(
            "/api/v1/schemas",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 201)
        data = json.loads(response.data)
        self.assertEqual(data["class_name"], "Company")
    
    def test_register_schema_missing_class_name(self):
        """Test registering schema without class_name"""
        payload = {"parent_class": "Thing"}
        response = self.client.post(
            "/api/v1/schemas",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 400)
    
    def test_get_class_hierarchy(self):
        """Test getting class hierarchy"""
        self.graph.schema_registry.register_class("Animal", parent_class="Thing")
        self.graph.schema_registry.register_class("Dog", parent_class="Animal")
        
        response = self.client.get("/api/v1/schemas/Dog/hierarchy", headers=self._get_auth_header())
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["hierarchy"], ["Dog", "Animal", "Thing"])
    
    # ===================== Node Routes Tests =====================
    
    def test_create_node(self):
        """Test creating a node"""
        payload = {
            "class_name": "Person",
            "name": "Alice",
            "attributes": {"age": 30, "city": "NYC"}
        }
        response = self.client.post(
            "/api/v1/nodes",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 201)
        data = json.loads(response.data)
        self.assertEqual(data["class_name"], "Person")
        self.assertEqual(data["name"], "Alice")
        self.assertEqual(data["attributes"]["age"], 30)
    
    def test_create_node_missing_name(self):
        """Test creating node without name"""
        payload = {"class_name": "Person"}
        response = self.client.post(
            "/api/v1/nodes",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 400)
    
    def test_get_node(self):
        """Test getting a node"""
        node = self.graph.create_node("Person", "Bob", {"age": 25})
        response = self.client.get(
            f"/api/v1/nodes/{node.node_id}",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["name"], "Bob")
    
    def test_get_nonexistent_node(self):
        """Test getting nonexistent node"""
        response = self.client.get(
            "/api/v1/nodes/nonexistent",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 404)
    
    def test_update_node(self):
        """Test updating a node"""
        node = self.graph.create_node("Person", "Alice", {"age": 30})
        payload = {
            "name": "Alice Updated",
            "attributes": {"age": 31, "city": "LA"}
        }
        response = self.client.put(
            f"/api/v1/nodes/{node.node_id}",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["name"], "Alice Updated")
        self.assertEqual(data["attributes"]["age"], 31)
    
    def test_update_nonexistent_node(self):
        """Test updating nonexistent node"""
        response = self.client.put(
            "/api/v1/nodes/nonexistent",
            json={"name": "test"},
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 404)
    
    def test_delete_node(self):
        """Test deleting a node"""
        node = self.graph.create_node("Person", "Alice")
        response = self.client.delete(
            f"/api/v1/nodes/{node.node_id}",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 204)
        self.assertIsNone(self.graph.get_node(node.node_id))
    
    def test_delete_nonexistent_node(self):
        """Test deleting nonexistent node"""
        response = self.client.delete(
            "/api/v1/nodes/nonexistent",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 404)
    
    def test_list_nodes(self):
        """Test listing all nodes"""
        self.graph.create_node("Person", "Alice", {"age": 30})
        self.graph.create_node("Person", "Bob", {"age": 25})
        
        response = self.client.get(
            "/api/v1/nodes",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["count"], 2)
    
    def test_list_nodes_by_class(self):
        """Test listing nodes by class"""
        self.graph.create_node("Person", "Alice", {"age": 30})
        self.graph.create_node("Person", "Bob", {"age": 25})
        
        response = self.client.get(
            "/api/v1/nodes?class_name=Person",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["count"], 2)
    
    # ===================== Edge Routes Tests =====================
    
    def test_create_edge(self):
        """Test creating an edge"""
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        
        payload = {
            "from_node_id": alice.node_id,
            "to_node_id": bob.node_id,
            "edge_type": "knows",
            "attributes": {"since": 2020}
        }
        response = self.client.post(
            "/api/v1/edges",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 201)
        data = json.loads(response.data)
        self.assertEqual(data["edge_type"], "knows")
    
    def test_create_edge_missing_nodes(self):
        """Test creating edge with missing from_node_id"""
        payload = {"to_node_id": "test"}
        response = self.client.post(
            "/api/v1/edges",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 400)
    
    def test_create_edge_invalid_nodes(self):
        """Test creating edge with invalid nodes"""
        payload = {
            "from_node_id": "nonexistent1",
            "to_node_id": "nonexistent2",
            "edge_type": "knows"
        }
        response = self.client.post(
            "/api/v1/edges",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 400)
    
    def test_get_edge(self):
        """Test getting an edge"""
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        edge = self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        
        response = self.client.get(
            f"/api/v1/edges/{edge.edge_id}",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["edge_type"], "knows")
    
    def test_get_nonexistent_edge(self):
        """Test getting nonexistent edge"""
        response = self.client.get(
            "/api/v1/edges/nonexistent",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 404)
    
    def test_delete_edge(self):
        """Test deleting an edge"""
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        edge = self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        
        response = self.client.delete(
            f"/api/v1/edges/{edge.edge_id}",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 204)
        self.assertIsNone(self.graph.get_edge(edge.edge_id))
    
    def test_get_node_edges_outgoing(self):
        """Test getting outgoing edges from node"""
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        charlie = self.graph.create_node("Person", "Charlie")
        
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(alice.node_id, charlie.node_id, "knows")
        
        response = self.client.get(
            f"/api/v1/nodes/{alice.node_id}/edges?direction=out",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["count"], 2)
    
    def test_get_node_edges_incoming(self):
        """Test getting incoming edges to node"""
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        charlie = self.graph.create_node("Person", "Charlie")
        
        self.graph.create_edge(bob.node_id, alice.node_id, "knows")
        self.graph.create_edge(charlie.node_id, alice.node_id, "knows")
        
        response = self.client.get(
            f"/api/v1/nodes/{alice.node_id}/edges?direction=in",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["count"], 2)
    
    def test_get_node_edges_both(self):
        """Test getting all edges for node"""
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        charlie = self.graph.create_node("Person", "Charlie")
        
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(charlie.node_id, alice.node_id, "knows")
        
        response = self.client.get(
            f"/api/v1/nodes/{alice.node_id}/edges?direction=both",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["count"], 2)
    
    def test_get_node_edges_by_type(self):
        """Test getting edges by type"""
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        charlie = self.graph.create_node("Person", "Charlie")
        
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(alice.node_id, charlie.node_id, "works_with")
        
        response = self.client.get(
            f"/api/v1/nodes/{alice.node_id}/edges?edge_type=knows",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["count"], 1)
    
    # ===================== Query Routes Tests =====================
    
    def test_query_paths(self):
        """Test finding paths between nodes"""
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        
        payload = {
            "start_node_id": alice.node_id,
            "end_node_id": bob.node_id,
            "max_depth": 10
        }
        response = self.client.post(
            "/api/v1/query/paths",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertGreater(data["path_count"], 0)
    
    def test_query_paths_missing_params(self):
        """Test query paths with missing parameters"""
        payload = {"start_node_id": "test"}
        response = self.client.post(
            "/api/v1/query/paths",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 400)
    
    def test_query_related_nodes(self):
        """Test finding related nodes"""
        alice = self.graph.create_node("Person", "Alice")
        bob = self.graph.create_node("Person", "Bob")
        
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        
        response = self.client.get(
            f"/api/v1/query/related/{alice.node_id}?direction=out",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertGreater(data["related_count"], 0)
    
    def test_query_related_nonexistent_node(self):
        """Test querying related for nonexistent node"""
        response = self.client.get(
            "/api/v1/query/related/nonexistent",
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 404)
    
    # ===================== Filter Routes Tests =====================
    
    def test_filter_connected_nodes(self):
        """Test filtering connected nodes"""
        alice = self.graph.create_node("Person", "Alice", {"age": 30})
        bob = self.graph.create_node("Person", "Bob", {"age": 25})
        charlie = self.graph.create_node("Person", "Charlie", {"age": 35})
        
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(alice.node_id, charlie.node_id, "is_a")
        
        payload = {}
        response = self.client.post(
            f"/api/v1/filter/connected/{alice.node_id}",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["count"], 1)
    
    def test_filter_connected_with_attribute_filter(self):
        """Test filtering connected nodes with attribute filter"""
        alice = self.graph.create_node("Person", "Alice", {"age": 30})
        bob = self.graph.create_node("Person", "Bob", {"age": 25})
        charlie = self.graph.create_node("Person", "Charlie", {"age": 35})
        
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(alice.node_id, charlie.node_id, "knows")
        
        payload = {
            "filter": {
                "filters": [{"attribute": "age", "operator": ">=", "value": 30}],
                "logic": "AND"
            }
        }
        response = self.client.post(
            f"/api/v1/filter/connected/{alice.node_id}",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["count"], 1)
    
    def test_filter_traverse(self):
        """Test traversing with filters"""
        alice = self.graph.create_node("Person", "Alice", {"age": 30})
        bob = self.graph.create_node("Person", "Bob", {"age": 25})
        charlie = self.graph.create_node("Person", "Charlie", {"age": 35})
        
        self.graph.create_edge(alice.node_id, bob.node_id, "knows")
        self.graph.create_edge(bob.node_id, charlie.node_id, "knows")
        
        payload = {
            "filter": {
                "filters": [{"attribute": "age", "operator": ">=", "value": 30}],
                "logic": "AND"
            },
            "max_depth": 5
        }
        response = self.client.post(
            f"/api/v1/filter/traverse/{alice.node_id}",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertGreater(data["count"], 0)
    
    def test_filter_by_class(self):
        """Test filtering nodes by class"""
        self.graph.create_node("Person", "Alice", {"age": 30})
        self.graph.create_node("Person", "Bob", {"age": 25})
        self.graph.create_node("Person", "Charlie", {"age": 35})
        
        payload = {
            "filter": {
                "filters": [{"attribute": "age", "operator": ">=", "value": 30}],
                "logic": "AND"
            }
        }
        response = self.client.post(
            "/api/v1/filter/by-class/Person",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["count"], 2)
    
    def test_filter_all_nodes(self):
        """Test filtering all nodes"""
        self.graph.create_node("Person", "Alice", {"age": 30})
        self.graph.create_node("Person", "Bob", {"age": 25})
        self.graph.create_node("Person", "Charlie", {"age": 35})
        
        payload = {
            "filter": {
                "filters": [{"attribute": "age", "operator": ">", "value": 25}],
                "logic": "AND"
            }
        }
        response = self.client.post(
            "/api/v1/filter/all",
            json=payload,
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["count"], 2)
    
    def test_filter_all_without_filter(self):
        """Test filter all without providing filter"""
        response = self.client.post(
            "/api/v1/filter/all",
            json={},
            headers=self._get_auth_header()
        )
        self.assertEqual(response.status_code, 400)
    
    # ===================== Authentication Tests =====================
    
    def test_create_node_requires_auth(self):
        """Test that create node requires authentication"""
        payload = {
            "class_name": "Person",
            "name": "Alice",
            "attributes": {"age": 30}
        }
        response = self.client.post("/api/v1/nodes", json=payload)
        self.assertEqual(response.status_code, 401)
    
    def test_invalid_token(self):
        """Test with invalid token"""
        response = self.client.get(
            "/api/v1/stats",
            headers={"Authorization": "Bearer invalid_token"}
        )
        self.assertEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()
