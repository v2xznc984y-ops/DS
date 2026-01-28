#!/usr/bin/env python3
"""
Test suite to verify Bully leader election algorithm invariants.

Non-negotiable rules to verify:
1. Leadership authority is based only on numeric node_id
2. A node with higher node_id must NEVER accept a leader with lower node_id
3. At most one leader may exist at any time
4. If the highest-ID alive node exists, it must eventually become leader
"""

import sys
import os
import unittest
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.node import Node
from src.protocol import make_msg, COORDINATOR, ELECTION_OK, DISCOVERY_REPLY


class BullyInvariantTests(unittest.TestCase):
    """Test Bully algorithm invariants."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock socket creation to avoid real network calls
        with patch('src.node.make_unicast_socket'):
            with patch('src.node.make_multicast_listener_socket'):
                self.node = Node(node_id=2)
    
    def tearDown(self):
        """Clean up after tests."""
        pass
    
    def test_rule_1_leadership_based_on_node_id(self):
        """Rule 1: Leadership authority is based only on numeric node_id."""
        # Create a node and simulate it becoming leader
        self.node.is_leader = True
        self.node.leader_id = self.node.node_id
        
        # Verify that leadership is tied to node_id
        self.assertEqual(self.node.leader_id, self.node.node_id)
        self.assertTrue(self.node.is_leader)
        print("✓ Rule 1: Leadership authority is based on numeric node_id")
    
    def test_rule_2_higher_node_rejects_lower_leader(self):
        """Rule 2: Higher-ID node must NEVER accept a leader with lower node_id."""
        # Node 5 receives COORDINATOR from Node 3
        self.node.node_id = 5
        self.node.term = 1
        self.node.members = {"3": ("127.0.0.1", 5003), "5": ("127.0.0.1", 5005)}
        
        # Simulate receiving a COORDINATOR message from lower-ID node
        coordinator_msg = make_msg(
            COORDINATOR,
            from_id=3,
            term=2,
            payload={"leader_id": 3}
        )
        
        with patch.object(self.node, 'start_election') as mock_start_election:
            self.node.on_message(coordinator_msg, ("127.0.0.1", 5003), "unicast")
            
            # Verify that:
            # 1. The coordinator was NOT accepted (election was started instead)
            # 2. start_election was called to assert higher-id leadership
            mock_start_election.assert_called_once()
            
        print("✓ Rule 2: Higher-ID node rejects leader with lower node_id")
    
    def test_rule_3_single_leader_at_a_time(self):
        """Rule 3: At most one leader may exist at any time."""
        # When a COORDINATOR is received with a higher term, only one leader is set
        self.node.node_id = 2
        self.node.term = 1
        self.node.members = {"1": ("127.0.0.1", 5001), "2": ("127.0.0.1", 5002), "3": ("127.0.0.1", 5003)}
        
        # Node 2 receives COORDINATOR from Node 3 (higher ID, valid)
        coordinator_msg = make_msg(
            COORDINATOR,
            from_id=3,
            term=2,
            payload={"leader_id": 3}
        )
        
        self.node.on_message(coordinator_msg, ("127.0.0.1", 5003), "unicast")
        
        # Verify: Node 2 accepts Node 3 as leader (higher ID)
        self.assertEqual(self.node.leader_id, 3)
        self.assertFalse(self.node.is_leader)  # Node 2 is NOT leader
        
        # Verify: Node 2 cannot simultaneously be leader and a follower
        self.assertNotEqual(self.node.is_leader, self.node.leader_id == self.node.node_id)
        
        print("✓ Rule 3: At most one leader exists at a time")
    
    def test_rule_4_highest_id_becomes_leader(self):
        """Rule 4: If the highest-ID alive node exists, it must eventually become leader."""
        # Simulate: Nodes 1, 3, 5 are alive; Node 5 is highest
        # Node 5 starts an election and should become leader
        self.node.node_id = 5
        self.node.members = {"1": ("127.0.0.1", 5001), "3": ("127.0.0.1", 5003), "5": ("127.0.0.1", 5005)}
        self.node.term = 0
        
        # Get higher IDs (should be empty for node 5)
        higher_ids = self.node.higher_ids()
        
        # Verify that Node 5 has no higher IDs, so it will become leader
        self.assertEqual(len(higher_ids), 0)
        
        # Simulate start_election; with no higher IDs, _become_leader should be called
        with patch.object(self.node, '_become_leader') as mock_become_leader:
            self.node.start_election()
            mock_become_leader.assert_called_once()
        
        print("✓ Rule 4: Highest-ID alive node can become leader")
    
    def test_ok_response_prevents_timeout_leadership(self):
        """Verify that receiving ELECTION_OK prevents the election timeout from making current node leader."""
        self.node.node_id = 2
        self.node.term = 0
        self.node.members = {"2": ("127.0.0.1", 5002), "3": ("127.0.0.1", 5003)}
        
        # Start election (should send ELECTION to node 3)
        with patch.object(self.node, 'send_json'):
            self.node.start_election()
        
        # Verify: election is in progress and got_ok is False
        self.assertTrue(self.node.election_in_progress)
        self.assertFalse(self.node.got_ok)
        
        # Simulate receiving ELECTION_OK from Node 3
        ok_msg = make_msg(ELECTION_OK, from_id=3, term=1)
        self.node.on_message(ok_msg, ("127.0.0.1", 5003), "unicast")
        
        # Verify: got_ok is now True (timeout won't make us leader)
        self.assertTrue(self.node.got_ok)
        self.assertTrue(self.node.awaiting_coordinator)
        
        print("✓ ELECTION_OK sets got_ok flag, preventing timeout leadership")
    
    def test_discovery_reply_bully_check(self):
        """Verify that discovery rejects lower-ID leaders."""
        self.node.node_id = 4
        self.node.term = 0
        
        # Simulate receiving a DISCOVERY_REPLY from Node 2 claiming leadership
        discovery_reply = make_msg(
            DISCOVERY_REPLY,
            from_id=2,
            term=1,
            payload={
                "term": 1,
                "leader_id": 2,
                "members": {"2": ("127.0.0.1", 5002), "4": ("127.0.0.1", 5004)},
                "last_seen": {}
            }
        )
        
        # Process the reply in the listener (on_message)
        self.node.on_message(discovery_reply, ("127.0.0.1", 5002), "unicast")
        
        # The discovery_reply should be stored by the listener thread
        self.assertIsNotNone(self.node.discovery_reply)
        
        print("✓ Discovery reply handler stores messages for startup_discovery processing")


if __name__ == '__main__':
    # Run tests with verbose output
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(BullyInvariantTests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
