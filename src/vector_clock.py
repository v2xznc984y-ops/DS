"""
Vector Clock implementation for causal ordering in distributed systems.

A vector clock is a mechanism for generating a partial ordering of events in a
distributed system and detecting causality violations.

Each node maintains a vector of logical clocks, one for each node in the system.
When a node processes an event, it increments its own clock in the vector.
When a node sends a message, it includes its vector clock.
When a node receives a message, it updates its vector clock by taking the max of
its current clock and the received clock for each component, then increments its own.
"""


class VectorClock:
    """
    Vector clock for a node in a distributed system.
    
    Attributes:
        node_id: ID of the node this clock belongs to
        clock: Dictionary mapping node_id -> logical_time
    """
    
    def __init__(self, node_id, members=None):
        """
        Initialize a vector clock for a node.
        
        Args:
            node_id: ID of this node
            members: Optional list of all member node IDs to initialize clock
        """
        self.node_id = node_id
        self.clock = {}
        
        if members:
            # Initialize all members' clocks to 0
            for member_id in members:
                self.clock[str(member_id)] = 0
        else:
            # Initialize just this node's clock
            self.clock[str(node_id)] = 0
    
    def increment(self):
        """
        Increment this node's logical clock.
        
        Called when this node processes an event (e.g., sends a message).
        """
        node_key = str(self.node_id)
        if node_key not in self.clock:
            self.clock[node_key] = 0
        self.clock[node_key] += 1
    
    def update(self, received_clock):
        """
        Update vector clock based on received message's clock.
        
        This implements the causal ordering rule:
        1. For each node in the received clock, take max(local_clock, received_clock)
        2. Then increment this node's own clock
        
        Args:
            received_clock: Dictionary of node_id -> logical_time from received message
        """
        # Update each component with the maximum
        for node_id_str, received_time in received_clock.items():
            if node_id_str not in self.clock:
                self.clock[node_id_str] = 0
            self.clock[node_id_str] = max(self.clock[node_id_str], received_time)
        
        # Increment this node's own clock
        self.increment()
    
    def get_clock(self):
        """
        Get a snapshot of the current vector clock.
        
        Returns:
            Dictionary copy of the current clock state
        """
        return dict(self.clock)
    
    def set_clock(self, clock_dict):
        """
        Set the vector clock from a dictionary.
        
        Args:
            clock_dict: Dictionary of node_id -> logical_time
        """
        self.clock = dict(clock_dict)
    
    def happens_before(self, other_clock):
        """
        Determine if this clock happens-before another clock.
        
        Returns True if this event causally precedes the other event.
        
        Args:
            other_clock: Dictionary of node_id -> logical_time
            
        Returns:
            Boolean: True if self < other, False otherwise
        """
        # Check if all our components are <= other's components
        # and at least one is strictly less
        all_le = True
        at_least_one_lt = False
        
        for node_id_str, time_val in self.clock.items():
            other_val = other_clock.get(node_id_str, 0)
            if time_val > other_val:
                all_le = False
                break
            if time_val < other_val:
                at_least_one_lt = True
        
        return all_le and at_least_one_lt
    
    def concurrent(self, other_clock):
        """
        Determine if two clocks are concurrent (neither happens-before the other).
        
        Args:
            other_clock: Dictionary of node_id -> logical_time
            
        Returns:
            Boolean: True if clocks are concurrent, False otherwise
        """
        # Get vector clock as snapshot for comparison
        vc = VectorClock(self.node_id)
        vc.set_clock(self.get_clock())
        
        # If neither happens-before the other, they're concurrent
        return not vc.happens_before(other_clock) and not self._other_happens_before(other_clock)
    
    def _other_happens_before(self, other_clock):
        """
        Determine if other_clock happens-before this clock.
        
        Args:
            other_clock: Dictionary of node_id -> logical_time
            
        Returns:
            Boolean: True if other < self, False otherwise
        """
        all_le = True
        at_least_one_lt = False
        
        for node_id_str, time_val in other_clock.items():
            our_val = self.clock.get(node_id_str, 0)
            if time_val > our_val:
                all_le = False
                break
            if time_val < our_val:
                at_least_one_lt = True
        
        return all_le and at_least_one_lt
    
    def __str__(self):
        """String representation of vector clock."""
        # Sort by node_id for consistent display
        items = sorted(self.clock.items(), key=lambda x: int(x[0]))
        return "{" + ", ".join(f"{k}:{v}" for k, v in items) + "}"
    
    def __repr__(self):
        """Representation of vector clock."""
        return f"VectorClock({self.node_id}, {self.clock})"
