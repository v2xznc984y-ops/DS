import sys
import os
import argparse
import time
import uuid

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.node import Node
from src.middleware_layer import MiddlewareLayer
from src.application_layer import ApplicationLayer


def main():
    """Main entry point for distributed system node with layered architecture."""
    parser = argparse.ArgumentParser(description="Distributed system node")
    parser.add_argument("--id", type=str, required=False, help="Optional node ID (if omitted a UUID will be generated)")
    
    args = parser.parse_args()
    
    # Generate UUID if --id not provided
    node_id = args.id if args.id else str(uuid.uuid4())
    
    print(f"Starting Node {node_id}...")
    
    # Initialize layered architecture
    # Core layer (networking and consensus)
    core_node = Node(node_id)
    
    # Middleware layer (ordering and membership)
    middleware = MiddlewareLayer(core_node)
    
    # Application layer (user interface)
    app = ApplicationLayer(middleware)
    
    # Connect layers
    core_node.application_layer = app
    core_node.middleware_layer = middleware
    
    # Start layers in order
    core_node.start()          # Start core distributed system
    middleware.start()         # Start middleware
    app.initialize(node_id)    # Initialize application
    app.start()                # Start application event processing
    
    print(f"Node {node_id} running. Press Ctrl+C to stop.")
    print(f"Layers: Core → Middleware → Application")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nNode {node_id} shutting down...")
        app.stop()
        middleware.stop()
        sys.exit(0)


if __name__ == "__main__":
    main()

