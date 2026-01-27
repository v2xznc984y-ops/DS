"""
Example Application - Demonstrating the layered architecture.

This example shows how to build an application on top of the distributed system
using the Application Layer and Middleware Layer APIs.
"""

import sys
import os
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.node import Node
from src.middleware_layer import MiddlewareLayer
from src.application_layer import ApplicationLayer


class ChatApplication:
    """
    Example distributed chat application built on the layered architecture.
    """
    
    def __init__(self, node_id):
        """Initialize the chat application."""
        # Core layer
        self.core_node = Node(node_id)
        
        # Middleware layer
        self.middleware = MiddlewareLayer(self.core_node)
        
        # Application layer
        self.app = ApplicationLayer(self.middleware)
        
        # Connect layers
        self.core_node.application_layer = self.app
        self.core_node.middleware_layer = self.middleware
        
        # Register callbacks for UI updates
        self.app.register_callback("on_message_received", self._on_message_received)
        self.app.register_callback("on_leader_elected", self._on_leader_elected)
        self.app.register_callback("on_node_joined", self._on_node_joined)
        
    def start(self):
        """Start the chat application."""
        print(f"\n=== Distributed Chat Application ===")
        print(f"Node ID: {self.core_node.node_id[:8]}...")
        print(f"Initializing layers...")
        
        # Start layers in order
        self.core_node.start()
        self.middleware.start()
        self.app.initialize(self.core_node.node_id)
        self.app.start()
        
        # Wait for cluster to stabilize
        print(f"Waiting for cluster to stabilize...")
        if self.middleware.wait_for_cluster_stable(min_members=1, timeout=15.0):
            print(f"✓ Cluster is stable!")
            print(f"  Leader: {self.middleware.get_leader()[:8]}...")
        else:
            print(f"✗ Cluster failed to stabilize")
        
        # Start message receiving thread
        self._start_receiver()
        
        # Show menu
        self._show_menu()
    
    def _start_receiver(self):
        """Start thread to receive messages."""
        def receive_loop():
            while True:
                try:
                    msg = self.app.receive_message(timeout=1.0)
                    if msg:
                        # Handled by callback
                        pass
                except Exception as e:
                    pass
        
        receiver_thread = threading.Thread(target=receive_loop, daemon=True)
        receiver_thread.start()
    
    def _on_message_received(self, msg):
        """Callback when a message is received."""
        sender = msg.get("sender_id", "Unknown")[:8]
        content = msg.get("content", "")
        timestamp = time.strftime("%H:%M:%S", time.localtime(msg.get("timestamp", 0)))
        print(f"\n[{timestamp}] {sender}... > {content}")
    
    def _on_leader_elected(self, event):
        """Callback when a leader is elected."""
        leader_id = event.get("leader_id", "Unknown")[:8]
        print(f"\n→ Leader elected: {leader_id}...")
    
    def _on_node_joined(self, event):
        """Callback when a node joins."""
        node_id = event.get("node_id", "Unknown")[:8]
        print(f"\n→ Node joined: {node_id}...")
    
    def _show_menu(self):
        """Show interactive menu."""
        print(f"\n=== Commands ===")
        print(f"  Type message and press Enter to send")
        print(f"  's' - Show status")
        print(f"  'm' - Show cluster members")
        print(f"  'h' - Show help")
        print(f"  'q' - Quit")
        print(f"==================\n")
        
        while True:
            try:
                user_input = input("> ").strip()
                
                if not user_input:
                    continue
                
                if user_input == 'q':
                    print("Shutting down...")
                    break
                
                elif user_input == 's':
                    self._show_status()
                
                elif user_input == 'm':
                    self._show_members()
                
                elif user_input == 'h':
                    self._show_menu()
                
                else:
                    # Send message
                    self.app.send_message(user_input)
            
            except KeyboardInterrupt:
                print("\nShutting down...")
                break
            except Exception as e:
                print(f"Error: {e}")
    
    def _show_status(self):
        """Show node status."""
        status = self.app.get_node_status()
        print(f"\n=== Node Status ===")
        print(f"  Node ID:        {status['node_id'][:8]}...")
        print(f"  Is Leader:      {status['is_leader']}")
        print(f"  Current Leader: {status['leader_id'][:8] if status['leader_id'] else 'None'}...")
        print(f"  Cluster Size:   {len(status['cluster_members'])}")
        print(f"  Middleware:")
        
        stats = self.middleware.get_stats()
        print(f"    - Term:       {stats['term']}")
        print(f"    - Messages:   Sent={stats['stats']['sent']}, Delivered={stats['stats']['delivered']}")
        print()
    
    def _show_members(self):
        """Show cluster members."""
        members = self.app.get_cluster_members()
        print(f"\n=== Cluster Members ({len(members)} total) ===")
        
        for member_id, info in members.items():
            is_me = "← You" if member_id == self.core_node.node_id else ""
            is_leader = "★ LEADER" if member_id == self.middleware.get_leader() else ""
            print(f"  {member_id[:8]}... {is_me} {is_leader}")
        print()


def main():
    """Main entry point for chat application."""
    import argparse
    import uuid
    
    parser = argparse.ArgumentParser(description="Distributed Chat Application")
    parser.add_argument("--id", type=str, required=False, 
                       help="Optional node ID (if omitted a UUID will be generated)")
    
    args = parser.parse_args()
    node_id = args.id if args.id else str(uuid.uuid4())
    
    try:
        # Create and start chat application
        chat = ChatApplication(node_id)
        chat.start()
    except KeyboardInterrupt:
        print(f"\nApplication terminated")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
