# ============================================================================
# File: sample_app.py
# Simple Sample Application for Graph System
# ============================================================================

import json
import requests
from typing import Dict, List, Any


class GraphClient:
    """Simple client for Graph REST API"""
    
    def __init__(self, base_url: str, token: str):
        """
        Initialize client
        
        Args:
            base_url: Base URL of Graph API (e.g., http://localhost:5000)
            token: Bearer token for authentication
        """
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def _get(self, endpoint: str) -> Dict[str, Any]:
        """GET request"""
        url = f"{self.base_url}/api/v1{endpoint}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def _post(self, endpoint: str, data: Dict) -> Dict[str, Any]:
        """POST request"""
        url = f"{self.base_url}/api/v1{endpoint}"
        response = requests.post(url, headers=self.headers, json=data)
        response.raise_for_status()
        return response.json()
    
    def create_node(self, class_name: str, name: str, attributes: Dict = None) -> Dict:
        """Create a node"""
        data = {
            "class_name": class_name,
            "name": name,
            "attributes": attributes or {}
        }
        return self._post("/nodes", data)
    
    def get_node(self, node_id: str) -> Dict:
        """Get node by ID"""
        return self._get(f"/nodes/{node_id}")
    
    def create_edge(self, from_node_id: str, to_node_id: str, edge_type: str, attributes: Dict = None) -> Dict:
        """Create an edge"""
        data = {
            "from_node_id": from_node_id,
            "to_node_id": to_node_id,
            "edge_type": edge_type,
            "attributes": attributes or {}
        }
        return self._post("/edges", data)
    
    def get_related_nodes(self, node_id: str, direction: str = "both") -> Dict:
        """Get related nodes"""
        return self._get(f"/query/related/{node_id}?direction={direction}")
    
    def filter_by_class(self, class_name: str, filters: Dict = None) -> Dict:
        """Filter nodes by class"""
        data = {"filter": filters} if filters else {}
        return self._post(f"/filter/by-class/{class_name}", data)


def print_header(text: str):
    """Print formatted header"""
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}\n")


def print_node(node: Dict, indent: str = ""):
    """Print node information"""
    print(f"{indent}Node ID: {node['node_id']}")
    print(f"{indent}Name: {node['name']}")
    print(f"{indent}Class: {node['class_name']}")
    print(f"{indent}Attributes: {json.dumps(node['attributes'], indent=10)}")
    print(f"{indent}Created: {node['created_at']}")


def print_connection(source_name: str, target_name: str, relationship: str):
    """Print connection information"""
    print(f"  └─ {source_name} --[{relationship}]--> {target_name}")


def main():
    """Main sample application"""
    
    # Configuration
    API_BASE_URL = "http://localhost:5000"
    API_TOKEN = "sk-admin-secret-token-123456"
    
    print("\n" + "="*60)
    print("  Graph System - Sample Application")
    print("="*60)
    print(f"\nConnecting to Graph API at {API_BASE_URL}")
    print(f"Using token: {API_TOKEN[:20]}...")
    
    try:
        # Initialize client
        client = GraphClient(API_BASE_URL, API_TOKEN)
        
        # ====================================================================
        # Step 1: Register Person Schema (if not already registered)
        # ====================================================================
        print_header("Step 1: Register Person Schema")
        try:
            schema_data = {
                "class_name": "Person",
                "parent_class": "Thing",
                "attributes": {
                    "age": "int",
                    "email": "string",
                    "city": "string"
                },
                "description": "A person entity"
            }
            schema = client._post("/schemas", schema_data)
            print(f"✓ Schema registered: {schema['class_name']}")
            print(f"  Parent class: {schema['parent_class']}")
            print(f"  Attributes: {', '.join(schema['attributes'].keys())}")
        except requests.HTTPError as e:
            if e.response.status_code == 400:
                print("✓ Schema already exists (Person)")
            else:
                raise
        
        # ====================================================================
        # Step 2: Create Node - Bob
        # ====================================================================
        print_header("Step 2: Create Node - Bob")
        bob_data = {
            "class_name": "Person",
            "name": "Bob",
            "attributes": {
                "age": 28,
                "email": "bob@example.com",
                "city": "San Francisco"
            }
        }
        bob = client.create_node(**bob_data)
        print(f"✓ Node created successfully")
        print_node(bob, indent="  ")
        bob_id = bob["node_id"]
        
        # ====================================================================
        # Step 3: Create Node - Alice
        # ====================================================================
        print_header("Step 3: Create Node - Alice")
        alice_data = {
            "class_name": "Person",
            "name": "Alice",
            "attributes": {
                "age": 32,
                "email": "alice@example.com",
                "city": "New York"
            }
        }
        alice = client.create_node(**alice_data)
        print(f"✓ Node created successfully")
        print_node(alice, indent="  ")
        alice_id = alice["node_id"]
        
        # ====================================================================
        # Step 4: Create Edge - knows
        # ====================================================================
        print_header("Step 4: Create Edge - Bob knows Alice")
        edge_data = {
            "from_node_id": bob_id,
            "to_node_id": alice_id,
            "edge_type": "knows",
            "attributes": {
                "since": 2018,
                "context": "university"
            }
        }
        edge = client._post("/edges", edge_data)
        print(f"✓ Edge created successfully")
        print(f"  Edge ID: {edge['edge_id']}")
        print(f"  Type: {edge['edge_type']}")
        print(f"  From: {bob['name']} --> To: {alice['name']}")
        print(f"  Attributes: {json.dumps(edge['attributes'], indent=4)}")
        
        # ====================================================================
        # Step 5: Query Node Alice
        # ====================================================================
        print_header("Step 5: Query Node - Alice")
        alice_retrieved = client.get_node(alice_id)
        print(f"✓ Node retrieved successfully")
        print_node(alice_retrieved, indent="  ")
        
        # ====================================================================
        # Step 6: Find Connected Person Nodes
        # ====================================================================
        print_header("Step 6: Find All Connected Person Nodes")
        
        # First, get all connected nodes for Alice
        related_response = client.get_related_nodes(alice_id, direction="both")
        related_nodes = related_response.get("related", [])
        
        print(f"✓ Found {related_response['related_count']} connected node(s)")
        
        if related_nodes:
            print("\nConnected Person Nodes:")
            for connection in related_nodes:
                node = connection["node"]
                relationship = connection["relationship"]
                print(f"\n  Node Information:")
                print(f"    Name: {node['name']}")
                print(f"    Class: {node['class_name']}")
                print(f"    City: {node['attributes'].get('city', 'N/A')}")
                print(f"    Age: {node['attributes'].get('age', 'N/A')}")
                print(f"    Email: {node['attributes'].get('email', 'N/A')}")
                print_connection("Alice", node['name'], relationship)
        else:
            print("\n  No connected nodes found")
        
        # ====================================================================
        # Step 7: Alternative - Filter All Person Nodes
        # ====================================================================
        print_header("Step 7: Alternative - List All Person Nodes")
        
        all_persons = client.filter_by_class("Person")
        print(f"✓ Found {all_persons['count']} Person node(s)")
        
        for person in all_persons["nodes"]:
            print(f"\n  {person['name']}:")
            print(f"    ID: {person['node_id']}")
            print(f"    Age: {person['attributes'].get('age', 'N/A')}")
            print(f"    City: {person['attributes'].get('city', 'N/A')}")
            print(f"    Email: {person['attributes'].get('email', 'N/A')}")
        
        # ====================================================================
        # Summary
        # ====================================================================
        print_header("Summary")
        print(f"✓ Created 2 Person nodes: Bob, Alice")
        print(f"✓ Created 1 Edge: Bob knows Alice (since 2018)")
        print(f"✓ Successfully queried Alice node")
        print(f"✓ Retrieved {related_response['related_count']} connected Person node(s)")
        print(f"\nApplication completed successfully!\n")
        
    except requests.exceptions.ConnectionError:
        print(f"\n✗ ERROR: Could not connect to API at {API_BASE_URL}")
        print(f"  Make sure the Graph API server is running:")
        print(f"  python graph_api.py --host 127.0.0.1 --port 5000")
    except requests.exceptions.HTTPError as e:
        print(f"\n✗ HTTP Error: {e.response.status_code}")
        print(f"  Response: {e.response.text}")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
