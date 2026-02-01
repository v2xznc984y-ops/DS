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
    
    node = Node(args.id)
    node.start()  # Begin discovery and show GUI
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Save logs before exiting
        node.ui.save_logs()
        sys.exit(0)


if __name__ == "__main__":
    main()
