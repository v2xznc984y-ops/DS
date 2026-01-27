"""
Application Layer - High-level interface for user applications.

This layer provides a clean API for applications to interact with the distributed system.
It abstracts away the complexity of the middleware and core networking.
"""

import json
import threading
import time
import queue
from typing import Callable, Optional, Dict, Any


class ApplicationLayer:
    """
    Application Layer Interface.
    
    Provides a high-level API for applications to:
    - Send messages to the cluster
    - Receive messages from other nodes
    - Query cluster membership
    - Register callbacks for various events
    """
    
    def __init__(self, middleware_layer):
        """
        Initialize the application layer.
        
        Args:
            middleware_layer: Reference to the middleware layer instance
        """
        self.middleware = middleware_layer
        self.node_id = None
        
        # Message queues for application
        self.incoming_message_queue = queue.Queue()
        self.event_queue = queue.Queue()
        
        # Callbacks for various events
        self.callbacks = {
            "on_message_received": None,
            "on_node_joined": None,
            "on_node_left": None,
            "on_leader_elected": None,
            "on_leader_changed": None,
        }
        
        # Application state
        self.running = False
        self.message_handlers = {}
        
    def initialize(self, node_id: str):
        """
        Initialize the application layer with node ID.
        
        Args:
            node_id: Unique identifier for this node
        """
        self.node_id = node_id
        print(f"Application Layer initialized for node {self.node_id}")
    
    def start(self):
        """Start the application layer event processing."""
        self.running = True
        
        # Start event processing thread
        event_thread = threading.Thread(
            target=self._process_events,
            daemon=True,
            name=f"AppLayer-EventProcessor-{self.node_id}"
        )
        event_thread.start()
        
        print(f"Application Layer started for node {self.node_id}")
    
    def stop(self):
        """Stop the application layer."""
        self.running = False
        print(f"Application Layer stopped for node {self.node_id}")
    
    def send_message(self, text: str, message_type: str = "CHAT") -> bool:
        """
        Send a message to all nodes in the cluster.
        
        Args:
            text: Message content
            message_type: Type of message (default: "CHAT")
            
        Returns:
            bool: True if message was accepted, False otherwise
        """
        try:
            if not self.middleware:
                print(f"Error: Middleware layer not initialized")
                return False
            
            message = {
                "type": message_type,
                "content": text,
                "sender_id": self.node_id,
                "timestamp": time.time()
            }
            
            # Send through middleware
            self.middleware.send_message(message)
            print(f"AppLayer: Message sent: {text[:50]}...")
            return True
            
        except Exception as e:
            print(f"AppLayer Error sending message: {e}")
            return False
    
    def receive_message(self, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """
        Receive a message from another node (blocking).
        
        Args:
            timeout: Timeout in seconds (None = wait forever)
            
        Returns:
            dict: Message dictionary or None if timeout
        """
        try:
            return self.incoming_message_queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def register_callback(self, event_type: str, callback: Callable):
        """
        Register a callback for a specific event.
        
        Args:
            event_type: Type of event ("on_message_received", "on_node_joined", etc.)
            callback: Callable function to invoke when event occurs
        """
        if event_type in self.callbacks:
            self.callbacks[event_type] = callback
            print(f"AppLayer: Registered callback for {event_type}")
        else:
            print(f"AppLayer: Unknown event type {event_type}")
    
    def get_cluster_members(self) -> Dict[str, Any]:
        """
        Get current cluster membership.
        
        Returns:
            dict: Dictionary of {node_id: node_info}
        """
        if self.middleware:
            return self.middleware.get_cluster_members()
        return {}
    
    def get_leader(self) -> Optional[str]:
        """
        Get the current leader node ID.
        
        Returns:
            str: Node ID of current leader or None
        """
        if self.middleware:
            return self.middleware.get_leader()
        return None
    
    def is_leader(self) -> bool:
        """
        Check if this node is the current leader.
        
        Returns:
            bool: True if this node is the leader
        """
        if self.middleware:
            return self.middleware.is_leader()
        return False
    
    def get_node_status(self) -> Dict[str, Any]:
        """
        Get comprehensive status of this node.
        
        Returns:
            dict: Status information including role, term, members, etc.
        """
        status = {
            "node_id": self.node_id,
            "is_leader": self.is_leader(),
            "leader_id": self.get_leader(),
            "cluster_members": list(self.get_cluster_members().keys()),
            "timestamp": time.time()
        }
        
        if self.middleware:
            status.update({
                "term": self.middleware.get_term(),
                "message_count": self.middleware.get_message_count(),
            })
        
        return status
    
    def _process_events(self):
        """Internal: Process events from middleware and invoke callbacks."""
        while self.running:
            try:
                # Check for incoming messages
                try:
                    msg = self.middleware.get_next_delivered_message(timeout=0.5)
                    if msg:
                        self.incoming_message_queue.put(msg)
                        
                        # Invoke callback if registered
                        if self.callbacks["on_message_received"]:
                            try:
                                self.callbacks["on_message_received"](msg)
                            except Exception as e:
                                print(f"AppLayer: Error in message callback: {e}")
                
                except queue.Empty:
                    pass
                
                # Check for cluster membership changes
                try:
                    event = self.event_queue.get_nowait()
                    self._handle_event(event)
                except queue.Empty:
                    pass
                
            except Exception as e:
                print(f"AppLayer: Event processing error: {e}")
            
            time.sleep(0.1)
    
    def _handle_event(self, event: Dict[str, Any]):
        """Internal: Handle events from middleware."""
        event_type = event.get("type")
        
        if event_type == "node_joined":
            if self.callbacks["on_node_joined"]:
                self.callbacks["on_node_joined"](event)
        
        elif event_type == "node_left":
            if self.callbacks["on_node_left"]:
                self.callbacks["on_node_left"](event)
        
        elif event_type == "leader_elected":
            if self.callbacks["on_leader_elected"]:
                self.callbacks["on_leader_elected"](event)
        
        elif event_type == "leader_changed":
            if self.callbacks["on_leader_changed"]:
                self.callbacks["on_leader_changed"](event)
    
    def notify_message_received(self, message: Dict[str, Any]):
        """
        Internal: Called by middleware when a message is delivered.
        
        Args:
            message: The delivered message
        """
        self.incoming_message_queue.put(message)
    
    def notify_event(self, event: Dict[str, Any]):
        """
        Internal: Called by middleware when an event occurs.
        
        Args:
            event: The event dictionary
        """
        self.event_queue.put(event)
