# ============================================================================
# File: integrated_sample_app.py
# Integrated Sample Application - Object Store + Graph System
# ============================================================================

import json
import requests
import os
from typing import Dict, List, Any
from pathlib import Path


class ObjectStoreClient:
    """Client for Object Store REST API"""
    
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream"
        }
    
    def create_object(self, data: bytes, versioned: bool = False) -> Dict[str, Any]:
        """Create new object"""
        url = f"{self.base_url}/api/v1/objects"
        if versioned:
            url += "?versioned=true"
        
        response = requests.post(url, data=data, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def get_object(self, object_id: str, version: int = None) -> bytes:
        """Get object data"""
        url = f"{self.base_url}/api/v1/objects/{object_id}"
        if version:
            url += f"?version={version}"
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.content


class GraphClient:
    """Client for Graph REST API"""
    
    def __init__(self, base_url: str, token: str):
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
    
    def register_schema(self, class_name: str, parent_class: str = "Thing", 
                       attributes: Dict = None, description: str = "") -> Dict:
        """Register schema"""
        data = {
            "class_name": class_name,
            "parent_class": parent_class,
            "attributes": attributes or {},
            "description": description
        }
        return self._post("/schemas", data)
    
    def create_node(self, class_name: str, name: str, attributes: Dict = None) -> Dict:
        """Create node"""
        data = {
            "class_name": class_name,
            "name": name,
            "attributes": attributes or {}
        }
        return self._post("/nodes", data)
    
    def get_node(self, node_id: str) -> Dict:
        """Get node"""
        return self._get(f"/nodes/{node_id}")
    
    def create_edge(self, from_node_id: str, to_node_id: str, edge_type: str, 
                   attributes: Dict = None) -> Dict:
        """Create edge"""
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


class IntegratedApp:
    """Integrated Sample Application"""
    
    def __init__(self, graph_api_url: str, object_store_url: str, token: str):
        self.graph = GraphClient(graph_api_url, token)
        self.object_store = ObjectStoreClient(object_store_url, token)
        self.token = token
    
    def create_sample_picture(self, name: str) -> bytes:
        """Create a simple PNG-like binary for demo"""
        # Create a minimal PNG header and simple data
        png_header = bytes([137, 80, 78, 71, 13, 10, 26, 10])  # PNG signature
        # Add minimal IHDR chunk for a 1x1 white pixel PNG
        ihdr = bytes([
            0, 0, 0, 13,  # chunk length
            73, 72, 68, 82,  # "IHDR"
            0, 0, 0, 1,  # width
            0, 0, 0, 1,  # height
            8, 2, 0, 0, 0,  # bit depth, color type, etc.
            144, 119, 83, 222  # CRC (dummy)
        ])
        return png_header + ihdr
    
    def print_header(self, text: str):
        """Print formatted header"""
        print(f"\n{'='*70}")
        print(f"  {text}")
        print(f"{'='*70}\n")
    
    def print_node_info(self, node: Dict, indent: str = ""):
        """Print node information"""
        print(f"{indent}Node ID: {node['node_id']}")
        print(f"{indent}Name: {node['name']}")
        print(f"{indent}Class: {node['class_name']}")
        if node['attributes']:
            print(f"{indent}Attributes:")
            for key, value in node['attributes'].items():
                print(f"{indent}  {key}: {value}")
    
    def run(self):
        """Run the integrated sample application"""
        
        print("\n" + "="*70)
        print("  Integrated Sample Application")
        print("  Object Store + Graph System")
        print("="*70)
        
        try:
            # ====================================================================
            # Step 1: Register Schemas
            # ====================================================================
            self.print_header("Step 1: Register Schemas")
            
            try:
                person_schema = self.graph.register_schema(
                    "Person",
                    parent_class="Thing",
                    attributes={
                        "age": "int",
                        "email": "string",
                        "city": "string",
                        "metadata_object_id": "string",
                        "picture_node_id": "string"
                    },
                    description="A person with metadata and picture"
                )
                print(f"✓ Person schema registered")
            except requests.HTTPError as e:
                if e.response.status_code == 400:
                    print(f"✓ Person schema already exists")
                else:
                    raise
            
            try:
                picture_schema = self.graph.register_schema(
                    "Picture",
                    parent_class="Thing",
                    attributes={
                        "object_store_id": "string",
                        "mime_type": "string",
                        "filename": "string"
                    },
                    description="A picture stored in Object Store"
                )
                print(f"✓ Picture schema registered")
            except requests.HTTPError as e:
                if e.response.status_code == 400:
                    print(f"✓ Picture schema already exists")
                else:
                    raise
            
            # ====================================================================
            # Step 2: Upload Pictures to Object Store
            # ====================================================================
            self.print_header("Step 2: Upload Pictures to Object Store")
            
            print("Creating sample pictures...")
            alice_picture_data = self.create_sample_picture("Alice")
            bob_picture_data = self.create_sample_picture("Bob")
            
            alice_picture_obj = self.object_store.create_object(alice_picture_data)
            alice_picture_id = alice_picture_obj["object_id"]
            print(f"✓ Alice picture uploaded")
            print(f"  Object ID: {alice_picture_id}")
            print(f"  Size: {alice_picture_obj['metadata']['size']} bytes")
            
            bob_picture_obj = self.object_store.create_object(bob_picture_data)
            bob_picture_id = bob_picture_obj["object_id"]
            print(f"✓ Bob picture uploaded")
            print(f"  Object ID: {bob_picture_id}")
            print(f"  Size: {bob_picture_obj['metadata']['size']} bytes")
            
            # ====================================================================
            # Step 3: Upload Metadata to Object Store
            # ====================================================================
            self.print_header("Step 3: Upload Metadata JSON to Object Store")
            
            alice_metadata = {
                "name": "Alice",
                "age": 32,
                "email": "alice@example.com",
                "city": "New York",
                "bio": "Software Engineer",
                "interests": ["Python", "Graph Databases", "Machine Learning"],
                "created_date": "2025-10-17"
            }
            alice_metadata_json = json.dumps(alice_metadata, indent=2).encode('utf-8')
            alice_metadata_obj = self.object_store.create_object(alice_metadata_json)
            alice_metadata_id = alice_metadata_obj["object_id"]
            
            print(f"✓ Alice metadata uploaded")
            print(f"  Object ID: {alice_metadata_id}")
            print(f"  Content: {json.dumps(alice_metadata, indent=4)}")
            
            bob_metadata = {
                "name": "Bob",
                "age": 28,
                "email": "bob@example.com",
                "city": "San Francisco",
                "bio": "Data Scientist",
                "interests": ["Data Analysis", "Graph Theory", "Deep Learning"],
                "created_date": "2025-10-17"
            }
            bob_metadata_json = json.dumps(bob_metadata, indent=2).encode('utf-8')
            bob_metadata_obj = self.object_store.create_object(bob_metadata_json)
            bob_metadata_id = bob_metadata_obj["object_id"]
            
            print(f"✓ Bob metadata uploaded")
            print(f"  Object ID: {bob_metadata_id}")
            print(f"  Content: {json.dumps(bob_metadata, indent=4)}")
            
            # ====================================================================
            # Step 4: Create Picture Nodes in Graph
            # ====================================================================
            self.print_header("Step 4: Create Picture Nodes in Graph")
            
            alice_picture_node = self.graph.create_node(
                "Picture",
                "Alice's Picture",
                {
                    "object_store_id": alice_picture_id,
                    "mime_type": "image/png",
                    "filename": "alice.png"
                }
            )
            alice_picture_node_id = alice_picture_node["node_id"]
            print(f"✓ Alice picture node created")
            self.print_node_info(alice_picture_node, indent="  ")
            
            bob_picture_node = self.graph.create_node(
                "Picture",
                "Bob's Picture",
                {
                    "object_store_id": bob_picture_id,
                    "mime_type": "image/png",
                    "filename": "bob.png"
                }
            )
            bob_picture_node_id = bob_picture_node["node_id"]
            print(f"✓ Bob picture node created")
            self.print_node_info(bob_picture_node, indent="  ")
            
            # ====================================================================
            # Step 5: Create Person Nodes in Graph
            # ====================================================================
            self.print_header("Step 5: Create Person Nodes in Graph")
            
            alice_node = self.graph.create_node(
                "Person",
                "Alice",
                {
                    "age": 32,
                    "email": "alice@example.com",
                    "city": "New York",
                    "metadata_object_id": alice_metadata_id,
                    "picture_node_id": alice_picture_node_id
                }
            )
            alice_node_id = alice_node["node_id"]
            print(f"✓ Alice node created")
            self.print_node_info(alice_node, indent="  ")
            
            bob_node = self.graph.create_node(
                "Person",
                "Bob",
                {
                    "age": 28,
                    "email": "bob@example.com",
                    "city": "San Francisco",
                    "metadata_object_id": bob_metadata_id,
                    "picture_node_id": bob_picture_node_id
                }
            )
            bob_node_id = bob_node["node_id"]
            print(f"✓ Bob node created")
            self.print_node_info(bob_node, indent="  ")
            
            # ====================================================================
            # Step 6: Create Relationships
            # ====================================================================
            self.print_header("Step 6: Create Relationships")
            
            # Person -> Picture edge
            alice_has_picture = self.graph.create_edge(
                alice_node_id,
                alice_picture_node_id,
                "has_a",
                {"relationship": "profile_picture"}
            )
            print(f"✓ Created edge: Alice has_a Picture")
            print(f"  Edge Type: {alice_has_picture['edge_type']}")
            
            bob_has_picture = self.graph.create_edge(
                bob_node_id,
                bob_picture_node_id,
                "has_a",
                {"relationship": "profile_picture"}
            )
            print(f"✓ Created edge: Bob has_a Picture")
            print(f"  Edge Type: {bob_has_picture['edge_type']}")
            
            # Person -> Person edge
            knows_edge = self.graph.create_edge(
                bob_node_id,
                alice_node_id,
                "knows",
                {"since": 2018, "context": "university"}
            )
            print(f"✓ Created edge: Bob knows Alice")
            print(f"  Edge Type: {knows_edge['edge_type']}")
            
            # ====================================================================
            # Step 7: Query and Display
            # ====================================================================
            self.print_header("Step 7: Query and Display Full Profile")
            
            alice_retrieved = self.graph.get_node(alice_node_id)
            print(f"\nAlice's Complete Profile:")
            print(f"-" * 70)
            self.print_node_info(alice_retrieved, indent="  ")
            
            alice_metadata_id_from_node = alice_retrieved["attributes"]["metadata_object_id"]
            alice_picture_id_from_node = alice_retrieved["attributes"]["picture_node_id"]
            
            # Retrieve and display metadata
            print(f"\n  Metadata (from Object Store):")
            metadata_bytes = self.object_store.get_object(alice_metadata_id_from_node)
            metadata = json.loads(metadata_bytes.decode('utf-8'))
            print(f"    {json.dumps(metadata, indent=6)}")
            
            # Get picture node details
            picture_node = self.graph.get_node(alice_picture_id_from_node)
            picture_object_id = picture_node["attributes"]["object_store_id"]
            picture_filename = picture_node["attributes"]["filename"]
            
            print(f"\n  Picture Link:")
            print(f"    Node ID: {alice_picture_id_from_node}")
            print(f"    Object Store ID: {picture_object_id}")
            picture_url = f"http://localhost:5000/api/v1/objects/{picture_object_id}"
            print(f"    Access URL: {picture_url}")
            print(f"    Filename: {picture_filename}")
            print(f"    HTML Link: <a href='{picture_url}' download='{picture_filename}'>Download Alice's Picture</a>")
            
            # ====================================================================
            # Step 8: Display Connected Nodes
            # ====================================================================
            self.print_header("Step 8: Display Connected Nodes")
            
            print(f"\nNodes connected to Alice:")
            alice_related = self.graph.get_related_nodes(alice_node_id, direction="both")
            
            for relation in alice_related["related"]:
                node = relation["node"]
                rel_type = relation["relationship"]
                print(f"\n  └─ {node['name']} ({node['class_name']}) [{rel_type}]")
                
                if node['class_name'] == "Picture":
                    pic_obj_id = node["attributes"]["object_store_id"]
                    pic_filename = node["attributes"]["filename"]
                    pic_url = f"http://localhost:5000/api/v1/objects/{pic_obj_id}"
                    print(f"     └─ Picture: {pic_filename}")
                    print(f"     └─ Access: {pic_url}")
                else:
                    for key, value in node["attributes"].items():
                        if key not in ["metadata_object_id", "picture_node_id"]:
                            print(f"     └─ {key}: {value}")
            
            # ====================================================================
            # Step 9: Summary
            # ====================================================================
            self.print_header("Summary")
            
            print(f"✓ Successfully created integrated demo:")
            print(f"\n  Objects in Object Store:")
            print(f"    - alice.png (picture): {alice_picture_id}")
            print(f"    - alice_metadata.json: {alice_metadata_id}")
            print(f"    - bob.png (picture): {bob_picture_id}")
            print(f"    - bob_metadata.json: {bob_metadata_id}")
            print(f"\n  Nodes in Graph:")
            print(f"    - Person: Alice ({alice_node_id})")
            print(f"    - Picture: Alice's Picture ({alice_picture_node_id})")
            print(f"    - Person: Bob ({bob_node_id})")
            print(f"    - Picture: Bob's Picture ({bob_picture_node_id})")
            print(f"\n  Edges:")
            print(f"    - Alice has_a Picture")
            print(f"    - Bob has_a Picture")
            print(f"    - Bob knows Alice")
            print(f"\nApplication completed successfully!\n")
            
        except requests.exceptions.ConnectionError as e:
            print(f"\n✗ Connection Error: Could not connect to APIs")
            print(f"  Make sure both servers are running:")
            print(f"  - Graph API: python graph_api.py --port 5000")
            print(f"  - Object Store: python object_store.py server --port 5001")
        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    # Configuration
    GRAPH_API_URL = "http://localhost:5000"
    OBJECT_STORE_URL = "http://localhost:5001"
    API_TOKEN = "sk-admin-secret-token-123456"
    
    app = IntegratedApp(GRAPH_API_URL, OBJECT_STORE_URL, API_TOKEN)
    app.run()
