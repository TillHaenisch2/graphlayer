# ============================================================================
# File: graph_api.py
# REST API for Graph System with Flask
# ============================================================================

import json
import logging
from typing import Optional, Dict, List, Any
from pathlib import Path
from functools import wraps
from flask import Flask, request, jsonify
from datetime import datetime

from graph_system import (
    GraphStore, GraphQuery, GraphFilter, AttributeFilter, FilterExpression
)

logger = logging.getLogger("graph_api")


# ============================================================================
# Authentication & Decorators
# ============================================================================

class APITokenManager:
    """Manage API tokens for authentication"""
    
    def __init__(self, tokens: Dict[str, str]):
        """
        Initialize with token dict
        
        Args:
            tokens: Dict of {user: token} pairs
        """
        self.tokens = tokens
        self.valid_tokens = set(tokens.values())
    
    def is_valid(self, token: str) -> bool:
        """Check if token is valid"""
        return token in self.valid_tokens
    
    def add_token(self, user: str, token: str):
        """Add new token"""
        self.tokens[user] = token
        self.valid_tokens.add(token)
    
    def revoke_token(self, token: str):
        """Revoke token"""
        if token in self.valid_tokens:
            self.valid_tokens.remove(token)


# ============================================================================
# Error Handling
# ============================================================================

class GraphAPIError(Exception):
    """Base exception for Graph API"""
    def __init__(self, message: str, status_code: int = 400):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class ValidationError(GraphAPIError):
    """Validation error"""
    def __init__(self, message: str):
        super().__init__(message, 400)


class NotFoundError(GraphAPIError):
    """Resource not found"""
    def __init__(self, resource: str, identifier: str):
        msg = f"{resource} '{identifier}' not found"
        super().__init__(msg, 404)


class UnauthorizedError(GraphAPIError):
    """Unauthorized access"""
    def __init__(self):
        super().__init__("Unauthorized", 401)


# ============================================================================
# Request/Response Models
# ============================================================================

class NodeRequest:
    """Request model for node creation/update"""
    
    @staticmethod
    def from_json(data: Dict) -> 'NodeRequest':
        """Parse from JSON"""
        req = NodeRequest()
        req.class_name = data.get("class_name")
        req.name = data.get("name")
        req.attributes = data.get("attributes", {})
        
        if not req.class_name:
            raise ValidationError("class_name is required")
        if not req.name:
            raise ValidationError("name is required")
        
        return req
    
    def __init__(self):
        self.class_name: str = None
        self.name: str = None
        self.attributes: Dict[str, Any] = {}


class EdgeRequest:
    """Request model for edge creation"""
    
    @staticmethod
    def from_json(data: Dict) -> 'EdgeRequest':
        """Parse from JSON"""
        req = EdgeRequest()
        req.from_node_id = data.get("from_node_id")
        req.to_node_id = data.get("to_node_id")
        req.edge_type = data.get("edge_type", "custom")
        req.attributes = data.get("attributes", {})
        
        if not req.from_node_id:
            raise ValidationError("from_node_id is required")
        if not req.to_node_id:
            raise ValidationError("to_node_id is required")
        
        return req
    
    def __init__(self):
        self.from_node_id: str = None
        self.to_node_id: str = None
        self.edge_type: str = "custom"
        self.attributes: Dict[str, Any] = {}


class FilterRequest:
    """Request model for filters"""
    
    @staticmethod
    def from_json(data: Dict) -> FilterExpression:
        """Parse filter from JSON"""
        if not data:
            return None
        
        filters = []
        for f in data.get("filters", []):
            attribute = f.get("attribute")
            operator = f.get("operator")
            value = f.get("value")
            
            if not all([attribute, operator, value]):
                raise ValidationError("Filter must have attribute, operator, and value")
            
            try:
                filters.append(AttributeFilter(attribute, operator, value))
            except ValueError as e:
                raise ValidationError(str(e))
        
        if not filters:
            return None
        
        logic_op = data.get("logic", "AND")
        if logic_op not in ("AND", "OR"):
            raise ValidationError("logic must be AND or OR")
        
        return FilterExpression(filters, operator=logic_op)


# ============================================================================
# Graph REST API
# ============================================================================

class GraphAPI:
    """REST API wrapper for Graph System"""
    
    def __init__(self, graph_store: GraphStore, token_manager: APITokenManager):
        self.graph = graph_store
        self.tokens = token_manager
        self.query = GraphQuery(graph_store)
        self.filter_engine = GraphFilter(graph_store)
        
        self.app = Flask(__name__)
        self._setup_routes()
        self._setup_error_handlers()
    
    def _require_auth(self, f):
        """Decorator for token authentication"""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            token = request.headers.get("Authorization", "").replace("Bearer ", "")
            if not self.tokens.is_valid(token):
                return jsonify({"error": "Unauthorized"}), 401
            return f(*args, **kwargs)
        return decorated_function
    
    def _setup_error_handlers(self):
        """Setup error handlers"""
        @self.app.errorhandler(GraphAPIError)
        def handle_error(error):
            return jsonify({"error": error.message}), error.status_code
        
        @self.app.errorhandler(400)
        def handle_bad_request(error):
            return jsonify({"error": "Bad request"}), 400
        
        @self.app.errorhandler(404)
        def handle_not_found(error):
            return jsonify({"error": "Not found"}), 404
        
        @self.app.errorhandler(500)
        def handle_error_500(error):
            logger.error(f"Internal error: {error}")
            return jsonify({"error": "Internal server error"}), 500
    
    def _setup_routes(self):
        """Setup Flask routes"""
        
        # ===================== Schema Routes =====================
        
        @self.app.route("/api/v1/schemas", methods=["GET"])
        @self._require_auth
        def get_schemas():
            """Get all registered schemas"""
            schemas = self.graph.schema_registry.get_all_schemas()
            return jsonify({
                "schemas": {name: schema.to_dict() for name, schema in schemas.items()}
            }), 200
        
        @self.app.route("/api/v1/schemas/<class_name>", methods=["GET"])
        @self._require_auth
        def get_schema(class_name):
            """Get schema for specific class"""
            schema = self.graph.schema_registry.get_schema(class_name)
            if not schema:
                raise NotFoundError("Schema", class_name)
            return jsonify(schema.to_dict()), 200
        
        @self.app.route("/api/v1/schemas", methods=["POST"])
        @self._require_auth
        def register_schema():
            """Register new schema"""
            data = request.get_json()
            if not data:
                raise ValidationError("Request body is required")
            
            class_name = data.get("class_name")
            parent_class = data.get("parent_class", "Thing")
            attributes = data.get("attributes", {})
            description = data.get("description", "")
            
            if not class_name:
                raise ValidationError("class_name is required")
            
            try:
                schema = self.graph.schema_registry.register_class(
                    class_name, parent_class, attributes, description
                )
                return jsonify(schema.to_dict()), 201
            except ValueError as e:
                raise ValidationError(str(e))
        
        @self.app.route("/api/v1/schemas/<class_name>/hierarchy", methods=["GET"])
        @self._require_auth
        def get_class_hierarchy(class_name):
            """Get class hierarchy"""
            if not self.graph.schema_registry.get_schema(class_name):
                raise NotFoundError("Schema", class_name)
            
            hierarchy = self.graph.schema_registry.get_class_hierarchy(class_name)
            return jsonify({"hierarchy": hierarchy}), 200
        
        # ===================== Node Routes =====================
        
        @self.app.route("/api/v1/nodes", methods=["POST"])
        @self._require_auth
        def create_node():
            """Create new node"""
            data = request.get_json()
            if not data:
                raise ValidationError("Request body is required")
            
            try:
                node_req = NodeRequest.from_json(data)
                node = self.graph.create_node(node_req.class_name, node_req.name, node_req.attributes)
                return jsonify(node.to_dict()), 201
            except ValueError as e:
                raise ValidationError(str(e))
        
        @self.app.route("/api/v1/nodes/<node_id>", methods=["GET"])
        @self._require_auth
        def get_node(node_id):
            """Get node by ID"""
            node = self.graph.get_node(node_id)
            if not node:
                raise NotFoundError("Node", node_id)
            return jsonify(node.to_dict()), 200
        
        @self.app.route("/api/v1/nodes/<node_id>", methods=["PUT"])
        @self._require_auth
        def update_node(node_id):
            """Update node"""
            node = self.graph.get_node(node_id)
            if not node:
                raise NotFoundError("Node", node_id)
            
            data = request.get_json()
            if not data:
                raise ValidationError("Request body is required")
            
            name = data.get("name")
            attributes = data.get("attributes")
            
            updated = self.graph.update_node(node_id, name, attributes)
            return jsonify(updated.to_dict()), 200
        
        @self.app.route("/api/v1/nodes/<node_id>", methods=["DELETE"])
        @self._require_auth
        def delete_node(node_id):
            """Delete node"""
            success = self.graph.delete_node(node_id)
            if not success:
                raise NotFoundError("Node", node_id)
            return "", 204
        
        @self.app.route("/api/v1/nodes", methods=["GET"])
        @self._require_auth
        def list_nodes():
            """List nodes with optional filtering"""
            class_name = request.args.get("class_name")
            filter_data = request.args.get("filter")
            
            if class_name:
                nodes = self.graph.find_nodes_by_class(class_name)
                
                if filter_data:
                    try:
                        filter_json = json.loads(filter_data)
                        filter_expr = FilterRequest.from_json(filter_json)
                        if filter_expr:
                            nodes = [n for n in nodes if filter_expr.matches(n)]
                    except json.JSONDecodeError:
                        raise ValidationError("Invalid filter JSON")
            else:
                nodes = list(self.graph.nodes.values())
            
            return jsonify({
                "count": len(nodes),
                "nodes": [n.to_dict() for n in nodes]
            }), 200
        
        # ===================== Edge Routes =====================
        
        @self.app.route("/api/v1/edges", methods=["POST"])
        @self._require_auth
        def create_edge():
            """Create new edge"""
            data = request.get_json()
            if not data:
                raise ValidationError("Request body is required")
            
            try:
                edge_req = EdgeRequest.from_json(data)
                edge = self.graph.create_edge(
                    edge_req.from_node_id,
                    edge_req.to_node_id,
                    edge_req.edge_type,
                    edge_req.attributes
                )
                return jsonify(edge.to_dict()), 201
            except ValueError as e:
                raise ValidationError(str(e))
        
        @self.app.route("/api/v1/edges/<edge_id>", methods=["GET"])
        @self._require_auth
        def get_edge(edge_id):
            """Get edge by ID"""
            edge = self.graph.get_edge(edge_id)
            if not edge:
                raise NotFoundError("Edge", edge_id)
            return jsonify(edge.to_dict()), 200
        
        @self.app.route("/api/v1/edges/<edge_id>", methods=["DELETE"])
        @self._require_auth
        def delete_edge(edge_id):
            """Delete edge"""
            success = self.graph.delete_edge(edge_id)
            if not success:
                raise NotFoundError("Edge", edge_id)
            return "", 204
        
        @self.app.route("/api/v1/nodes/<node_id>/edges", methods=["GET"])
        @self._require_auth
        def get_node_edges(node_id):
            """Get all edges connected to node"""
            node = self.graph.get_node(node_id)
            if not node:
                raise NotFoundError("Node", node_id)
            
            direction = request.args.get("direction", "both")
            edge_type = request.args.get("edge_type")
            
            if direction == "out":
                edges = self.graph.get_outgoing_edges(node_id, edge_type)
            elif direction == "in":
                edges = self.graph.get_incoming_edges(node_id, edge_type)
            else:
                out_edges = self.graph.get_outgoing_edges(node_id, edge_type)
                in_edges = self.graph.get_incoming_edges(node_id, edge_type)
                edges = out_edges + in_edges
            
            return jsonify({
                "node_id": node_id,
                "direction": direction,
                "count": len(edges),
                "edges": [e.to_dict() for e in edges]
            }), 200
        
        # ===================== Query Routes =====================
        
        @self.app.route("/api/v1/query/paths", methods=["POST"])
        @self._require_auth
        def query_paths():
            """Find paths between two nodes"""
            data = request.get_json()
            if not data:
                raise ValidationError("Request body is required")
            
            start_node_id = data.get("start_node_id")
            end_node_id = data.get("end_node_id")
            max_depth = data.get("max_depth", 10)
            
            if not start_node_id or not end_node_id:
                raise ValidationError("start_node_id and end_node_id are required")
            
            paths = self.query.find_paths(start_node_id, end_node_id, max_depth)
            return jsonify({
                "start": start_node_id,
                "end": end_node_id,
                "path_count": len(paths),
                "paths": paths
            }), 200
        
        @self.app.route("/api/v1/query/related/<node_id>", methods=["GET"])
        @self._require_auth
        def query_related(node_id):
            """Find related nodes"""
            node = self.graph.get_node(node_id)
            if not node:
                raise NotFoundError("Node", node_id)
            
            direction = request.args.get("direction", "both")
            relationship_type = request.args.get("relationship_type")
            
            related = self.query.find_related_nodes(node_id, relationship_type, direction)
            return jsonify({
                "node_id": node_id,
                "direction": direction,
                "related_count": len(related),
                "related": [{"node": n.to_dict(), "relationship": rel_type} for n, rel_type in related]
            }), 200
        
        # ===================== Filter Routes =====================
        
        @self.app.route("/api/v1/filter/connected/<node_id>", methods=["POST"])
        @self._require_auth
        def filter_connected(node_id):
            """Get connected nodes excluding is_a"""
            node = self.graph.get_node(node_id)
            if not node:
                raise NotFoundError("Node", node_id)
            
            data = request.get_json() or {}
            filter_expr = FilterRequest.from_json(data.get("filter"))
            exclude_types = data.get("exclude_edge_types", ["is_a"])
            
            connected = self.filter_engine.get_connected_nodes(
                node_id,
                exclude_edge_types=exclude_types,
                attribute_filter=filter_expr
            )
            
            return jsonify({
                "node_id": node_id,
                "count": len(connected),
                "nodes": [n.to_dict() for n in connected]
            }), 200
        
        @self.app.route("/api/v1/filter/traverse/<node_id>", methods=["POST"])
        @self._require_auth
        def filter_traverse(node_id):
            """Traverse graph with filters"""
            node = self.graph.get_node(node_id)
            if not node:
                raise NotFoundError("Node", node_id)
            
            data = request.get_json() or {}
            filter_expr = FilterRequest.from_json(data.get("filter"))
            exclude_types = data.get("exclude_edge_types", ["is_a"])
            max_depth = data.get("max_depth", -1)
            
            traversed = self.filter_engine.traverse_with_filter(
                node_id,
                exclude_edge_types=exclude_types,
                attribute_filter=filter_expr,
                max_depth=max_depth
            )
            
            return jsonify({
                "node_id": node_id,
                "count": len(traversed),
                "nodes": [n.to_dict() for n in traversed]
            }), 200
        
        @self.app.route("/api/v1/filter/by-class/<class_name>", methods=["POST"])
        @self._require_auth
        def filter_by_class(class_name):
            """Filter nodes by class"""
            schema = self.graph.schema_registry.get_schema(class_name)
            if not schema:
                raise NotFoundError("Schema", class_name)
            
            data = request.get_json() or {}
            filter_expr = FilterRequest.from_json(data.get("filter"))
            
            nodes = self.filter_engine.filter_nodes_by_class(class_name, filter_expr)
            return jsonify({
                "class_name": class_name,
                "count": len(nodes),
                "nodes": [n.to_dict() for n in nodes]
            }), 200
        
        @self.app.route("/api/v1/filter/all", methods=["POST"])
        @self._require_auth
        def filter_all():
            """Filter all nodes"""
            data = request.get_json()
            if not data:
                raise ValidationError("Request body with filter is required")
            
            filter_expr = FilterRequest.from_json(data.get("filter"))
            if not filter_expr:
                raise ValidationError("No filters provided")
            
            nodes = self.filter_engine.filter_all_nodes(filter_expr)
            return jsonify({
                "count": len(nodes),
                "nodes": [n.to_dict() for n in nodes]
            }), 200
        
        # ===================== Health & Info Routes =====================
        
        @self.app.route("/api/v1/health", methods=["GET"])
        def health_check():
            """Health check endpoint"""
            return jsonify({
                "status": "ok",
                "timestamp": datetime.utcnow().isoformat(),
                "nodes": len(self.graph.nodes),
                "edges": len(self.graph.edges),
                "schemas": len(self.graph.schema_registry.get_all_schemas())
            }), 200
        
        @self.app.route("/api/v1/stats", methods=["GET"])
        @self._require_auth
        def get_stats():
            """Get graph statistics"""
            nodes = self.graph.nodes
            edges = self.graph.edges
            
            nodes_by_class = {}
            for node in nodes.values():
                if node.class_name not in nodes_by_class:
                    nodes_by_class[node.class_name] = 0
                nodes_by_class[node.class_name] += 1
            
            edges_by_type = {}
            for edge in edges.values():
                if edge.edge_type not in edges_by_type:
                    edges_by_type[edge.edge_type] = 0
                edges_by_type[edge.edge_type] += 1
            
            return jsonify({
                "total_nodes": len(nodes),
                "total_edges": len(edges),
                "total_schemas": len(self.graph.schema_registry.get_all_schemas()),
                "nodes_by_class": nodes_by_class,
                "edges_by_type": edges_by_type
            }), 200
    
    def run(self, host: str = "127.0.0.1", port: int = 5000, debug: bool = False):
        """Run the API server"""
        self.app.run(host=host, port=port, debug=debug)


# ============================================================================
# CLI Entry Point
# ============================================================================

if __name__ == "__main__":
    import argparse
    from pathlib import Path
    
    parser = argparse.ArgumentParser(description="Graph System REST API")
    parser.add_argument("--data-dir", default="./graph_data", help="Data directory")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    parser.add_argument("--debug", action="store_true", help="Debug mode")
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    # Initialize graph
    data_dir = Path(args.data_dir)
    graph = GraphStore(data_dir)
    
    # Setup tokens
    token_manager = APITokenManager({
        "admin": "sk-admin-secret-token-123456",
        "user": "sk-user-token-987654"
    })
    
    # Create API
    api = GraphAPI(graph, token_manager)
    
    print(f"Starting Graph API Server on {args.host}:{args.port}")
    print(f"Admin Token: sk-admin-secret-token-123456")
    print(f"User Token: sk-user-token-987654")
    print(f"Health Check: http://{args.host}:{args.port}/api/v1/health")
    
    api.run(host=args.host, port=args.port, debug=args.debug)
