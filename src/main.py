import sys
import os
import argparse
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.node import Node


def main():
    """Main entry point for distributed system node."""
    parser = argparse.ArgumentParser(description="Distributed system node")
    parser.add_argument("--id", type=int, required=True, help="Node ID")
    
    args = parser.parse_args()
    
    print(f"Starting Node {args.id}...")
    node = Node(args.id)
    node.start()  # Begin discovery
    
    print(f"Node {args.id} running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nNode {args.id} shutting down...")
        sys.exit(0)


if __name__ == "__main__":
    main()
