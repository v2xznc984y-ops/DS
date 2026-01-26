import sys
import os
import argparse
import time
import uuid

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.node import Node


def main():
    """Main entry point for distributed system node."""
    parser = argparse.ArgumentParser(description="Distributed system node")
    parser.add_argument("--id", type=str, required=False, help="Optional node ID (if omitted a UUID will be generated)")
    
    args = parser.parse_args()
    
    # Generate UUID if --id not provided
    node_id = args.id if args.id else str(uuid.uuid4())
    
    print(f"Starting Node {node_id}...")
    node = Node(node_id)
    node.start()  # Begin discovery
    
    print(f"Node {node_id} running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nNode {node_id} shutting down...")
        sys.exit(0)


if __name__ == "__main__":
    main()
