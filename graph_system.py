# ============================================================================
# File: graph_system.py
# Graph-based Relationship System with Class Hierarchies and Schemas
# ============================================================================

import json
import uuid
from typing import Optional, Dict, List, Set, Tuple, Any
from dataclasses import dataclass, asdict, field
from datetime import datetime
from enum import Enum
from pathlib import Path
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger("graph_system")


# ============================================================================
# Enums
# ============================================================================

class EdgeType(Enum):
    """Supported edge types"""
    IS_A = "is_a"              # Class hierarchy: child -> parent
    HAS_A = "has_a"            # Aggregation: parent -> child
    CUSTOM = "custom"          # User-defined relationships


# ============================================================================
# Models
# ============================================================================

@dataclass
class NodeSchema:
    """Schema definition for a node class"""
    class_name: str
    parent_class: Optional[str] = None  # For is_a hierarchy
    attributes: Dict[str, str] = field(default_factory=dict)  # attr_name -> type (str, int, bool, etc.)
    description: str = ""
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @staticmethod
    def from_dict(data: Dict) -> 'NodeSchema':
        return NodeSchema(**data)


@dataclass
class Node:
    """Graph node representing an entity"""
    node_id: str
    class_name: str
    name: str
    attributes: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    def to_dict(self) -> Dict:
        return {
            "node_id": self.node_id,
            "class_name": self.class_name,
            "name": self.name,
            "attributes": self.attributes,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }
    
    @staticmethod
    def from_dict(data: Dict) -> 'Node':
        return Node(**data)


@dataclass
class Edge:
    """Graph edge representing a relationship"""
    edge_id: str
    from_node_id: str
    to_node_id: str
    edge_type: str  # is_a, has_a, or custom type
    attributes: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    def to_dict(self) -> Dict:
        return {
            "edge_id": self.edge_id,
            "from_node_id": self.from_node_id,
            "to_node_id": self.to_node_id,
            "edge_type": self.edge_type,
            "attributes": self.attributes,
            "created_at": self.created_at
        }
    
    @staticmethod
    def from_dict(data: Dict) -> 'Edge':
        return Edge(**data)


# ============================================================================
# Schema Registry
# ============================================================================

class SchemaRegistry:
    """Manages node class schemas and hierarchy"""
    
    def __init__(self, storage_dir: Path):
        self.storage_dir = storage_dir
        self.schema_dir = storage_dir / "schemas"
        self.schema_dir.mkdir(parents=True, exist_ok=True)
        self.schemas: Dict[str, NodeSchema] = {}
        self._init_thing_schema()
        self._load_schemas()
    
    def _init_thing_schema(self):
        """Initialize base Thing schema"""
        thing_schema = NodeSchema(
            class_name="Thing",
            parent_class=None,
            attributes={
                "name": "string",
                "type": "string",
                "timestamp": "string"
            },
            description="Base class for all entities"
        )
        self.schemas["Thing"] = thing_schema
        self._save_schema(thing_schema)
    
    def _save_schema(self, schema: NodeSchema):
        """Save schema to disk"""
        schema_path = self.schema_dir / f"{schema.class_name}.json"
        with open(schema_path, 'w') as f:
            json.dump(schema.to_dict(), f, indent=2)
    
    def _load_schemas(self):
        """Load all schemas from disk"""
        for schema_file in self.schema_dir.glob("*.json"):
            with open(schema_file, 'r') as f:
                data = json.load(f)
                schema = NodeSchema.from_dict(data)
                self.schemas[schema.class_name] = schema
    
    def register_class(self, class_name: str, parent_class: str = "Thing",
                      attributes: Optional[Dict[str, str]] = None,
                      description: str = "") -> NodeSchema:
        """Register a new node class"""
        if class_name in self.schemas:
            raise ValueError(f"Class {class_name} already exists")
        
        if parent_class not in self.schemas:
            raise ValueError(f"Parent class {parent_class} not found")
        
        schema = NodeSchema(
            class_name=class_name,
            parent_class=parent_class,
            attributes=attributes or {},
            description=description
        )
        
        self.schemas[class_name] = schema
        self._save_schema(schema)
        logger.info(f"Registered class: {class_name}")
        return schema
    
    def get_schema(self, class_name: str) -> Optional[NodeSchema]:
        """Get schema for a class"""
        return self.schemas.get(class_name)
    
    def get_all_schemas(self) -> Dict[str, NodeSchema]:
        """Get all registered schemas"""
        return self.schemas.copy()
    
    def get_class_hierarchy(self, class_name: str) -> List[str]:
        """Get class hierarchy from child to root"""
        hierarchy = [class_name]
        current = class_name
        
        while current:
            schema = self.schemas.get(current)
            if not schema or not schema.parent_class:
                break
            hierarchy.append(schema.parent_class)
            current = schema.parent_class
        
        return hierarchy
    
    def is_subclass_of(self, child: str, parent: str) -> bool:
        """Check if child is a subclass of parent"""
        return parent in self.get_class_hierarchy(child)


# ============================================================================
# Graph Store
# ============================================================================

class GraphStore:
    """Manages graph nodes and edges"""
    
    def __init__(self, storage_dir: Path):
        self.storage_dir = storage_dir
        self.nodes_dir = storage_dir / "graph_nodes"
        self.edges_dir = storage_dir / "graph_edges"
        self.nodes_dir.mkdir(parents=True, exist_ok=True)
        self.edges_dir.mkdir(parents=True, exist_ok=True)
        
        self.schema_registry = SchemaRegistry(storage_dir)
        self.nodes: Dict[str, Node] = {}
        self.edges: Dict[str, Edge] = {}
        self._load_graph()
    
    def _get_node_path(self, node_id: str) -> Path:
        """Get path for node storage"""
        return self.nodes_dir / f"{node_id}.json"
    
    def _get_edge_path(self, edge_id: str) -> Path:
        """Get path for edge storage"""
        return self.edges_dir / f"{edge_id}.json"
    
    def _save_node(self, node: Node):
        """Save node to disk"""
        node_path = self._get_node_path(node.node_id)
        with open(node_path, 'w') as f:
            json.dump(node.to_dict(), f, indent=2)
    
    def _load_node(self, node_id: str) -> Optional[Node]:
        """Load node from disk"""
        node_path = self._get_node_path(node_id)
        if not node_path.exists():
            return None
        with open(node_path, 'r') as f:
            data = json.load(f)
            return Node.from_dict(data)
    
    def _save_edge(self, edge: Edge):
        """Save edge to disk"""
        edge_path = self._get_edge_path(edge.edge_id)
        with open(edge_path, 'w') as f:
            json.dump(edge.to_dict(), f, indent=2)
    
    def _load_edge(self, edge_id: str) -> Optional[Edge]:
        """Load edge from disk"""
        edge_path = self._get_edge_path(edge_id)
        if not edge_path.exists():
            return None
        with open(edge_path, 'r') as f:
            data = json.load(f)
            return Edge.from_dict(data)
    
    def _load_graph(self):
        """Load entire graph from disk"""
        # Load nodes
        for node_file in self.nodes_dir.glob("*.json"):
            with open(node_file, 'r') as f:
                data = json.load(f)
                node = Node.from_dict(data)
                self.nodes[node.node_id] = node
        
        # Load edges
        for edge_file in self.edges_dir.glob("*.json"):
            with open(edge_file, 'r') as f:
                data = json.load(f)
                edge = Edge.from_dict(data)
                self.edges[edge.edge_id] = edge
    
    def create_node(self, class_name: str, name: str,
                   attributes: Optional[Dict[str, Any]] = None) -> Node:
        """Create a new node"""
        schema = self.schema_registry.get_schema(class_name)
        if not schema:
            raise ValueError(f"Unknown class: {class_name}")
        
        node_id = f"{class_name}:{uuid.uuid4().hex[:16]}"
        timestamp = datetime.utcnow().isoformat()
        
        node = Node(
            node_id=node_id,
            class_name=class_name,
            name=name,
            attributes=attributes or {},
            created_at=timestamp,
            updated_at=timestamp
        )
        
        self.nodes[node_id] = node
        self._save_node(node)
        logger.info(f"Created node: {node_id}")
        return node
    
    def get_node(self, node_id: str) -> Optional[Node]:
        """Get node by ID"""
        return self.nodes.get(node_id)
    
    def update_node(self, node_id: str, name: Optional[str] = None,
                   attributes: Optional[Dict[str, Any]] = None) -> Optional[Node]:
        """Update node"""
        node = self.nodes.get(node_id)
        if not node:
            return None
        
        if name:
            node.name = name
        if attributes:
            node.attributes.update(attributes)
        
        node.updated_at = datetime.utcnow().isoformat()
        self._save_node(node)
        logger.info(f"Updated node: {node_id}")
        return node
    
    def delete_node(self, node_id: str) -> bool:
        """Delete node and all connected edges"""
        if node_id not in self.nodes:
            return False
        
        # Delete all edges connected to this node
        edges_to_delete = [
            edge_id for edge_id, edge in self.edges.items()
            if edge.from_node_id == node_id or edge.to_node_id == node_id
        ]
        
        for edge_id in edges_to_delete:
            self.delete_edge(edge_id)
        
        # Delete node
        del self.nodes[node_id]
        node_path = self._get_node_path(node_id)
        if node_path.exists():
            node_path.unlink()
        
        logger.info(f"Deleted node: {node_id}")
        return True
    
    def create_edge(self, from_node_id: str, to_node_id: str,
                   edge_type: str = "has_a",
                   attributes: Optional[Dict[str, Any]] = None) -> Edge:
        """Create an edge between two nodes"""
        if from_node_id not in self.nodes:
            raise ValueError(f"Source node {from_node_id} not found")
        if to_node_id not in self.nodes:
            raise ValueError(f"Target node {to_node_id} not found")
        
        edge_id = f"{from_node_id}_{edge_type}_{to_node_id}:{uuid.uuid4().hex[:8]}"
        timestamp = datetime.utcnow().isoformat()
        
        edge = Edge(
            edge_id=edge_id,
            from_node_id=from_node_id,
            to_node_id=to_node_id,
            edge_type=edge_type,
            attributes=attributes or {},
            created_at=timestamp
        )
        
        self.edges[edge_id] = edge
        self._save_edge(edge)
        logger.info(f"Created edge: {edge_id}")
        return edge
    
    def get_edge(self, edge_id: str) -> Optional[Edge]:
        """Get edge by ID"""
        return self.edges.get(edge_id)
    
    def delete_edge(self, edge_id: str) -> bool:
        """Delete an edge"""
        if edge_id not in self.edges:
            return False
        
        del self.edges[edge_id]
        edge_path = self._get_edge_path(edge_id)
        if edge_path.exists():
            edge_path.unlink()
        
        logger.info(f"Deleted edge: {edge_id}")
        return True
    
    def get_outgoing_edges(self, node_id: str, edge_type: Optional[str] = None) -> List[Edge]:
        """Get all outgoing edges from a node"""
        edges = [
            edge for edge in self.edges.values()
            if edge.from_node_id == node_id
        ]
        
        if edge_type:
            edges = [e for e in edges if e.edge_type == edge_type]
        
        return edges
    
    def get_incoming_edges(self, node_id: str, edge_type: Optional[str] = None) -> List[Edge]:
        """Get all incoming edges to a node"""
        edges = [
            edge for edge in self.edges.values()
            if edge.to_node_id == node_id
        ]
        
        if edge_type:
            edges = [e for e in edges if e.edge_type == edge_type]
        
        return edges
    
    def get_parent_classes(self, node_id: str) -> List[Node]:
        """Get parent classes via is_a edges"""
        edges = self.get_outgoing_edges(node_id, "is_a")
        return [self.nodes[edge.to_node_id] for edge in edges if edge.to_node_id in self.nodes]
    
    def get_aggregated_children(self, node_id: str) -> List[Node]:
        """Get aggregated children via has_a edges"""
        edges = self.get_outgoing_edges(node_id, "has_a")
        return [self.nodes[edge.to_node_id] for edge in edges if edge.to_node_id in self.nodes]
    
    def get_aggregating_parents(self, node_id: str) -> List[Node]:
        """Get nodes that aggregate this node via has_a edges"""
        edges = self.get_incoming_edges(node_id, "has_a")
        return [self.nodes[edge.from_node_id] for edge in edges if edge.from_node_id in self.nodes]
    
    def find_nodes_by_class(self, class_name: str) -> List[Node]:
        """Find all nodes of a specific class"""
        return [node for node in self.nodes.values() if node.class_name == class_name]
    
    def find_nodes_by_name(self, name: str) -> List[Node]:
        """Find all nodes with a specific name"""
        return [node for node in self.nodes.values() if node.name == name]
    
    def traverse_is_a_hierarchy(self, node_id: str) -> List[Node]:
        """Traverse is_a hierarchy upwards"""
        result = []
        current = node_id
        visited = set()
        
        while current and current not in visited:
            visited.add(current)
            if current in self.nodes:
                result.append(self.nodes[current])
                parents = self.get_parent_classes(current)
                if parents:
                    current = parents[0].node_id
                else:
                    break
            else:
                break
        
        return result
    
    def traverse_has_a_tree(self, node_id: str, depth: int = -1) -> Dict[str, Any]:
        """Traverse has_a aggregation tree"""
        if node_id not in self.nodes:
            return {}
        
        node = self.nodes[node_id]
        result = {
            "node_id": node_id,
            "name": node.name,
            "class_name": node.class_name,
            "children": []
        }
        
        if depth != 0:
            children = self.get_aggregated_children(node_id)
            for child in children:
                child_tree = self.traverse_has_a_tree(
                    child.node_id,
                    depth - 1 if depth > 0 else -1
                )
                result["children"].append(child_tree)
        
        return result


# ============================================================================
# Graph Query Engine
# ============================================================================

# ============================================================================
# Graph Query Engine
# ============================================================================

class AttributeFilter:
    """Filter for node attributes"""
    
    OPERATORS = {"==", "<=", ">=", "!=", "<", ">", "in", "contains"}
    
    def __init__(self, attribute: str, operator: str, value: Any):
        """
        Create an attribute filter
        
        Args:
            attribute: Attribute name to filter on
            operator: Comparison operator (==, <=, >=, !=, <, >, in, contains)
            value: Value to compare against
        """
        if operator not in self.OPERATORS:
            raise ValueError(f"Unsupported operator: {operator}")
        
        self.attribute = attribute
        self.operator = operator
        self.value = value
    
    def matches(self, node: Node) -> bool:
        """Check if node matches this filter"""
        if self.attribute not in node.attributes:
            return False
        
        node_value = node.attributes[self.attribute]
        
        if self.operator == "==":
            return node_value == self.value
        elif self.operator == "!=":
            return node_value != self.value
        elif self.operator == "<=":
            return node_value <= self.value
        elif self.operator == ">=":
            return node_value >= self.value
        elif self.operator == "<":
            return node_value < self.value
        elif self.operator == ">":
            return node_value > self.value
        elif self.operator == "in":
            # Check if node_value is in the list/set
            return node_value in self.value
        elif self.operator == "contains":
            # Check if value is substring of node_value (for strings)
            if isinstance(node_value, str) and isinstance(self.value, str):
                return self.value in node_value
            return False
        
        return False


class FilterExpression:
    """Composite filter expression (AND/OR logic)"""
    
    def __init__(self, filters: List[AttributeFilter] = None, operator: str = "AND"):
        """
        Create a filter expression
        
        Args:
            filters: List of AttributeFilter objects
            operator: "AND" or "OR"
        """
        if operator not in ("AND", "OR"):
            raise ValueError("Operator must be AND or OR")
        
        self.filters = filters or []
        self.operator = operator
    
    def add_filter(self, attribute_filter: AttributeFilter) -> 'FilterExpression':
        """Add a filter to the expression"""
        self.filters.append(attribute_filter)
        return self
    
    def matches(self, node: Node) -> bool:
        """Check if node matches this expression"""
        if not self.filters:
            return True
        
        if self.operator == "AND":
            return all(f.matches(node) for f in self.filters)
        else:  # OR
            return any(f.matches(node) for f in self.filters)


class GraphFilter:
    """Graph filtering engine"""
    
    def __init__(self, graph_store: GraphStore):
        self.graph = graph_store
    
    def get_connected_nodes(self, node_id: str, exclude_edge_types: Optional[List[str]] = None,
                           attribute_filter: Optional[FilterExpression] = None) -> List[Node]:
        """
        Get all nodes connected to a given node, excluding certain edge types
        
        Args:
            node_id: Starting node ID
            exclude_edge_types: List of edge types to exclude (e.g., ["is_a"])
            attribute_filter: Optional FilterExpression to apply
        
        Returns:
            List of connected nodes matching criteria
        """
        if node_id not in self.graph.nodes:
            return []
        
        exclude_types = exclude_edge_types or []
        connected = set()
        
        # Get all outgoing edges
        for edge in self.graph.get_outgoing_edges(node_id):
            if edge.edge_type not in exclude_types:
                connected.add(edge.to_node_id)
        
        # Get all incoming edges
        for edge in self.graph.get_incoming_edges(node_id):
            if edge.edge_type not in exclude_types:
                connected.add(edge.from_node_id)
        
        # Convert to nodes and apply attribute filter
        result = []
        for node_id_item in connected:
            node = self.graph.get_node(node_id_item)
            if node:
                if attribute_filter is None or attribute_filter.matches(node):
                    result.append(node)
        
        return result
    
    def get_connected_nodes_excluding_is_a(self, node_id: str,
                                          attribute_filter: Optional[FilterExpression] = None) -> List[Node]:
        """
        Get all connected nodes except those connected via is_a edges
        
        Args:
            node_id: Starting node ID
            attribute_filter: Optional FilterExpression to apply
        
        Returns:
            List of connected nodes (excluding is_a connections)
        """
        return self.get_connected_nodes(node_id, exclude_edge_types=["is_a"], attribute_filter=attribute_filter)
    
    def traverse_with_filter(self, start_node_id: str,
                           exclude_edge_types: Optional[List[str]] = None,
                           attribute_filter: Optional[FilterExpression] = None,
                           max_depth: int = -1) -> List[Node]:
        """
        Traverse graph from a node, collecting all reachable nodes
        
        Args:
            start_node_id: Starting node ID
            exclude_edge_types: Edge types to exclude
            attribute_filter: Optional FilterExpression to apply
            max_depth: Maximum traversal depth (-1 for unlimited)
        
        Returns:
            List of all reachable nodes matching criteria
        """
        from collections import deque
        
        if start_node_id not in self.graph.nodes:
            return []
        
        exclude_types = exclude_edge_types or []
        visited = set()
        result = []
        queue = deque([(start_node_id, 0)])
        
        while queue:
            current_id, depth = queue.popleft()
            
            if current_id in visited:
                continue
            
            if max_depth >= 0 and depth > max_depth:
                continue
            
            visited.add(current_id)
            node = self.graph.get_node(current_id)
            
            if node:
                # Apply filter
                if attribute_filter is None or attribute_filter.matches(node):
                    result.append(node)
                
                # Add neighbors to queue
                for edge in self.graph.get_outgoing_edges(current_id):
                    if edge.edge_type not in exclude_types and edge.to_node_id not in visited:
                        queue.append((edge.to_node_id, depth + 1))
                
                for edge in self.graph.get_incoming_edges(current_id):
                    if edge.edge_type not in exclude_types and edge.from_node_id not in visited:
                        queue.append((edge.from_node_id, depth + 1))
        
        return result
    
    def filter_nodes_by_class(self, class_name: str,
                             attribute_filter: Optional[FilterExpression] = None) -> List[Node]:
        """
        Filter all nodes by class and optional attributes
        
        Args:
            class_name: Class name to filter by
            attribute_filter: Optional FilterExpression
        
        Returns:
            List of matching nodes
        """
        nodes = self.graph.find_nodes_by_class(class_name)
        
        if attribute_filter is None:
            return nodes
        
        return [node for node in nodes if attribute_filter.matches(node)]
    
    def filter_all_nodes(self, attribute_filter: FilterExpression) -> List[Node]:
        """
        Filter all nodes in the graph by attribute filter
        
        Args:
            attribute_filter: FilterExpression to apply
        
        Returns:
            List of all matching nodes
        """
        return [node for node in self.graph.nodes.values() if attribute_filter.matches(node)]


class GraphQuery:
    """Query engine for graph operations"""
    
    def __init__(self, graph_store: GraphStore):
        self.graph = graph_store
    
    def find_paths(self, start_node_id: str, end_node_id: str,
                  max_depth: int = 10) -> List[List[str]]:
        """Find all paths between two nodes (BFS)"""
        from collections import deque
        
        if start_node_id not in self.graph.nodes:
            return []
        
        queue = deque([(start_node_id, [start_node_id])])
        paths = []
        visited_states = set()
        
        while queue:
            current, path = queue.popleft()
            
            if len(path) > max_depth:
                continue
            
            state = (current, tuple(path))
            if state in visited_states:
                continue
            visited_states.add(state)
            
            if current == end_node_id:
                paths.append(path)
                continue
            
            # Get all outgoing edges
            for edge in self.graph.get_outgoing_edges(current):
                next_node = edge.to_node_id
                if next_node not in path:  # Avoid cycles
                    queue.append((next_node, path + [next_node]))
        
        return paths
    
    def find_related_nodes(self, node_id: str, relationship_type: Optional[str] = None,
                          direction: str = "both") -> List[Tuple[Node, str]]:
        """Find nodes related to a given node"""
        results = []
        
        if direction in ("out", "both"):
            for edge in self.graph.get_outgoing_edges(node_id, relationship_type):
                target = self.graph.get_node(edge.to_node_id)
                if target:
                    results.append((target, edge.edge_type))
        
        if direction in ("in", "both"):
            for edge in self.graph.get_incoming_edges(node_id, relationship_type):
                source = self.graph.get_node(edge.from_node_id)
                if source:
                    results.append((source, edge.edge_type))
        
        return results
    
    def get_nodes_by_class_and_attribute(self, class_name: str,
                                        attr_name: str,
                                        attr_value: Any) -> List[Node]:
        """Find nodes by class and attribute value"""
        nodes = self.graph.find_nodes_by_class(class_name)
        return [n for n in nodes if n.attributes.get(attr_name) == attr_value]


if __name__ == "__main__":
    # Example usage
    import tempfile
    import logging
    
    logging.basicConfig(level=logging.INFO)
    
    temp_dir = Path(tempfile.mkdtemp())
    graph = GraphStore(temp_dir)
    
    # Register classes
    graph.schema_registry.register_class(
        "Person",
        parent_class="Thing",
        attributes={"age": "int", "email": "string"}
    )
    
    graph.schema_registry.register_class(
        "Organization",
        parent_class="Thing",
        attributes={"industry": "string"}
    )
    
    # Create nodes
    alice = graph.create_node("Person", "Alice", {"age": 30, "email": "alice@example.com"})
    bob = graph.create_node("Person", "Bob", {"age": 25})
    acme = graph.create_node("Organization", "ACME Corp", {"industry": "tech"})
    
    # Create edges
    graph.create_edge(bob.node_id, alice.node_id, "knows", {"since": 2020})
    graph.create_edge(alice.node_id, acme.node_id, "works_at")
    
    # Query
    query = GraphQuery(graph)
    paths = query.find_paths(bob.node_id, acme.node_id)
    print(f"Paths from {bob.node_id} to {acme.node_id}: {paths}")
    
    related = query.find_related_nodes(alice.node_id)
    print(f"Nodes related to Alice: {[(n.name, rel_type) for n, rel_type in related]}")
    
    # Filter test
    graph_filter = GraphFilter(graph)
    connected = graph_filter.get_connected_nodes_excluding_is_a(alice.node_id)
    print(f"Connected to Alice (excluding is_a): {[n.name for n in connected]}")