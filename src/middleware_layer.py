"""
Middleware Layer - Bridges application layer with the core distributed system.

This layer handles:
- Message ordering and delivery guarantees
- Cluster membership management
- Leader election coordination
- Fault tolerance mechanisms
- Communication with the core node layer
"""

import json
import threading
import time
import queue
from typing import Dict, Optional, Any, List
from src.protocol import ORDERED, DELIVER_ACK


class MiddlewareLayer:
    """
    Middleware Layer Interface.
    
    Provides abstraction between application layer and core distributed system.
    Handles ordering, delivery guarantees, and cluster coordination.
    """
    
    def __init__(self, core_node):
        """
        Initialize the middleware layer.
        
        Args:
            core_node: Reference to the core Node instance
        """
        self.core_node = core_node
        self.node_id = core_node.node_id
        
        # Message queues for delivery
        self.delivered_messages_queue = queue.Queue()
        
        # Tracking
        self.message_stats = {
            "sent": 0,
            "received": 0,
            "delivered": 0,
            "pending": 0
        }
        
        self.running = False
        
    def start(self):
        """Start the middleware layer."""
        self.running = True
        
        # Start message delivery thread
        delivery_thread = threading.Thread(
            target=self._message_delivery_loop,
            daemon=True,
            name=f"MiddlewareLayer-Delivery-{self.node_id}"
        )
        delivery_thread.start()
        
        print(f"Middleware Layer started for node {self.node_id}")
    
    def stop(self):
        """Stop the middleware layer."""
        self.running = False
        print(f"Middleware Layer stopped for node {self.node_id}")
    
    def send_message(self, message: Dict[str, Any]):
        """
        Send a message through the distributed system.
        
        Args:
            message: Message dictionary to send
        """
        try:
            # Wrap message for transmission
            wrapped_msg = {
                "type": "APP_MESSAGE",
                "payload": message,
                "sender_id": self.node_id,
                "timestamp": time.time()
            }
            
            # Send through core node's multicast ordering
            self.core_node.propose_message(wrapped_msg)
            self.message_stats["sent"] += 1
            
        except Exception as e:
            print(f"Middleware: Error sending message: {e}")
    
    def get_next_delivered_message(self, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """
        Get the next delivered message (in order).
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            dict: Next delivered message or None
        """
        try:
            return self.delivered_messages_queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def notify_message_delivered(self, message: Dict[str, Any]):
        """
        Internal: Called by core node when a message is delivered to application.
        
        Args:
            message: The delivered message
        """
        self.delivered_messages_queue.put(message)
        self.message_stats["delivered"] += 1
    
    def get_cluster_members(self) -> Dict[str, Any]:
        """
        Get current cluster membership information.
        
        Returns:
            dict: Dictionary of {node_id: {address, status, last_seen}}
        """
        members_info = {}
        
        for member_id, addr in self.core_node.members.items():
            members_info[member_id] = {
                "address": addr,
                "last_seen": self.core_node.last_seen.get(member_id, 0),
                "status": "active"  # Could be expanded to include health status
            }
        
        return members_info
    
    def get_leader(self) -> Optional[str]:
        """
        Get the current leader node ID.
        
        Returns:
            str: Leader node ID or None
        """
        return self.core_node.leader_id
    
    def is_leader(self) -> bool:
        """
        Check if this node is the leader.
        
        Returns:
            bool: True if this node is the leader
        """
        return self.core_node.is_leader
    
    def get_term(self) -> int:
        """
        Get the current election term.
        
        Returns:
            int: Current term number
        """
        return self.core_node.term
    
    def get_message_count(self) -> int:
        """
        Get total number of messages seen by this node.
        
        Returns:
            int: Message count
        """
        return self.core_node.next_seq_to_deliver
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get middleware statistics.
        
        Returns:
            dict: Statistics including message counts, delivery rates, etc.
        """
        return {
            "node_id": self.node_id,
            "is_leader": self.is_leader(),
            "term": self.get_term(),
            "cluster_size": len(self.core_node.members),
            "stats": self.message_stats.copy()
        }
    
    def wait_for_leader_election(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for a leader to be elected in the cluster.
        
        Args:
            timeout: Timeout in seconds (None = wait forever)
            
        Returns:
            bool: True if leader was elected, False if timeout
        """
        start_time = time.time()
        
        while self.running:
            if self.core_node.leader_id and self.core_node.leader_id != "NULL":
                return True
            
            if timeout and (time.time() - start_time) > timeout:
                return False
            
            time.sleep(0.1)
        
        return False
    
    def wait_for_cluster_stable(self, min_members: int = 2, timeout: Optional[float] = None) -> bool:
        """
        Wait for the cluster to stabilize with minimum members.
        
        Args:
            min_members: Minimum cluster size needed
            timeout: Timeout in seconds (None = wait forever)
            
        Returns:
            bool: True if cluster is stable, False if timeout
        """
        start_time = time.time()
        
        while self.running:
            if len(self.core_node.members) >= min_members:
                # Check if leader exists
                if self.core_node.leader_id and self.core_node.leader_id != "NULL":
                    return True
            
            if timeout and (time.time() - start_time) > timeout:
                return False
            
            time.sleep(0.1)
        
        return False
    
    def trigger_election(self):
        """
        Manually trigger a leader election.
        
        This is primarily for testing/debugging purposes.
        """
        print(f"Middleware: Triggering manual election for node {self.node_id}")
        self.core_node.start_election()
    
    def _message_delivery_loop(self):
        """Internal: Main loop for message processing and delivery."""
        while self.running:
            try:
                # Check for new delivered messages from core node
                # This would be called when ORDERED messages are ready for delivery
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Middleware: Error in delivery loop: {e}")
    
    def get_message_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get recent message history (leader only).
        
        Args:
            limit: Maximum number of messages to return
            
        Returns:
            list: List of recent messages
        """
        if not self.is_leader():
            return []
        
        # Return core node's message history
        return self.core_node.message_history[-limit:]
    
    def get_node_role(self) -> str:
        """
        Get this node's current role in the cluster.
        
        Returns:
            str: "LEADER", "FOLLOWER", or "CANDIDATE"
        """
        if self.is_leader():
            return "LEADER"
        elif self.core_node.election_in_progress:
            return "CANDIDATE"
        else:
            return "FOLLOWER"
