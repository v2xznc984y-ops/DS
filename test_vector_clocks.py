#!/usr/bin/env python3
"""
Test suite for Vector Clock implementation.

Tests the causal ordering and vector clock operations in the distributed system.
"""

import sys
import os
import unittest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.vector_clock import VectorClock


class VectorClockTests(unittest.TestCase):
    """Test Vector Clock operations."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.vc1 = VectorClock(node_id=1, members=[1, 2, 3])
        self.vc2 = VectorClock(node_id=2, members=[1, 2, 3])
        self.vc3 = VectorClock(node_id=3, members=[1, 2, 3])
    
    def test_initialization(self):
        """Test vector clock initialization."""
        vc = VectorClock(node_id=1, members=[1, 2, 3])
        self.assertEqual(vc.get_clock(), {"1": 0, "2": 0, "3": 0})
        print("✓ Vector clock initialization correct")
    
    def test_increment(self):
        """Test incrementing a vector clock."""
        vc = VectorClock(node_id=1, members=[1, 2, 3])
        vc.increment()
        self.assertEqual(vc.get_clock()["1"], 1)
        vc.increment()
        self.assertEqual(vc.get_clock()["1"], 2)
        print("✓ Vector clock increment correct")
    
    def test_update_and_increment(self):
        """Test updating vector clock from received message."""
        vc1 = VectorClock(node_id=1, members=[1, 2, 3])
        vc2 = VectorClock(node_id=2, members=[1, 2, 3])
        
        # Node 1 sends message (increments clock)
        vc1.increment()
        msg_clock = vc1.get_clock()  # {1: 1, 2: 0, 3: 0}
        
        # Node 2 receives message (updates and increments)
        vc2.update(msg_clock)
        result = vc2.get_clock()
        
        # Node 2's clock should be: {1: max(0,1), 2: 0+1, 3: max(0,0)} = {1: 1, 2: 1, 3: 0}
        self.assertEqual(result["1"], 1)
        self.assertEqual(result["2"], 1)
        self.assertEqual(result["3"], 0)
        print("✓ Vector clock update and increment correct")
    
    def test_happens_before(self):
        """Test happens-before relationship."""
        vc1 = VectorClock(node_id=1, members=[1, 2, 3])
        vc2 = VectorClock(node_id=2, members=[1, 2, 3])
        
        # Node 1 sends
        vc1.increment()  # {1: 1, 2: 0, 3: 0}
        
        # Node 2 receives and updates
        vc2.update(vc1.get_clock())  # {1: 1, 2: 1, 3: 0}
        
        # vc1 happens-before vc2
        self.assertTrue(vc1.happens_before(vc2.get_clock()))
        # vc2 does NOT happen-before vc1
        self.assertFalse(vc2.happens_before(vc1.get_clock()))
        print("✓ Happens-before relationship correct")
    
    def test_concurrent_events(self):
        """Test detection of concurrent events."""
        vc1 = VectorClock(node_id=1, members=[1, 2, 3])
        vc2 = VectorClock(node_id=2, members=[1, 2, 3])
        
        # Both increment independently (no communication)
        vc1.increment()  # {1: 1, 2: 0, 3: 0}
        vc2.increment()  # {1: 0, 2: 1, 3: 0}
        
        # Neither happens-before the other -> concurrent
        self.assertTrue(vc1.concurrent(vc2.get_clock()))
        self.assertTrue(vc2.concurrent(vc1.get_clock()))
        print("✓ Concurrent event detection correct")
    
    def test_causal_chain(self):
        """Test causal chain: A -> B -> C."""
        vc_a = VectorClock(node_id=1, members=[1, 2, 3])
        vc_b = VectorClock(node_id=2, members=[1, 2, 3])
        vc_c = VectorClock(node_id=3, members=[1, 2, 3])
        
        # A: Node 1 sends
        vc_a.increment()  # A: {1:1, 2:0, 3:0}
        
        # B: Node 2 receives A and sends
        vc_b.update(vc_a.get_clock())  # B: {1:1, 2:1, 3:0}
        
        # C: Node 3 receives B
        vc_c.update(vc_b.get_clock())  # C: {1:1, 2:1, 3:1}
        
        # Verify causal chain
        self.assertTrue(vc_a.happens_before(vc_b.get_clock()))
        self.assertTrue(vc_b.happens_before(vc_c.get_clock()))
        self.assertTrue(vc_a.happens_before(vc_c.get_clock()))
        print("✓ Causal chain preserved correctly")
    
    def test_set_and_get_clock(self):
        """Test setting and getting clock state."""
        vc = VectorClock(node_id=1, members=[1, 2, 3])
        
        # Set custom clock
        custom_clock = {"1": 5, "2": 3, "3": 2}
        vc.set_clock(custom_clock)
        
        # Verify it's set correctly
        self.assertEqual(vc.get_clock(), custom_clock)
        print("✓ Set and get clock state correct")
    
    def test_string_representation(self):
        """Test string representation of vector clock."""
        vc = VectorClock(node_id=1, members=[1, 2, 3])
        vc.increment()
        vc.increment()
        
        vc_str = str(vc)
        self.assertIn("1:2", vc_str)
        self.assertIn("2:0", vc_str)
        self.assertIn("3:0", vc_str)
        print(f"✓ String representation correct: {vc_str}")
    
    def test_multiple_updates(self):
        """Test multiple sequential updates."""
        vc = VectorClock(node_id=1, members=[1, 2, 3])
        
        # Update 1
        msg1 = {"1": 2, "2": 1, "3": 0}
        vc.update(msg1)
        result1 = vc.get_clock()
        self.assertEqual(result1["1"], 2)
        self.assertEqual(result1["2"], 1)
        self.assertEqual(result1["1"], 2)  # Did increment
        
        # Update 2
        msg2 = {"1": 1, "2": 2, "3": 1}
        vc.update(msg2)
        result2 = vc.get_clock()
        self.assertEqual(result2["1"], 2)  # max(2, 1)
        self.assertEqual(result2["2"], 2)  # max(1, 2), then increment
        self.assertEqual(result2["3"], 1)  # max(0, 1)
        print("✓ Multiple updates correct")


class VectorClockIntegrationTests(unittest.TestCase):
    """Integration tests for vector clocks with Node."""
    
    def test_vector_clock_in_message(self):
        """Test that vector clock can be serialized/deserialized in message."""
        vc = VectorClock(node_id=1, members=[1, 2, 3])
        vc.increment()
        vc.increment()
        
        # Simulate message encoding
        clock_dict = vc.get_clock()
        
        # Simulate message decoding
        new_vc = VectorClock(node_id=2, members=[1, 2, 3])
        new_vc.update(clock_dict)
        
        # Verify the clock was transmitted and updated correctly
        result = new_vc.get_clock()
        self.assertEqual(result["1"], 2)
        self.assertEqual(result["2"], 1)  # Incremented by receiver
        print("✓ Vector clock message encoding/decoding correct")


def run_tests():
    """Run all vector clock tests."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(VectorClockTests))
    suite.addTests(loader.loadTestsFromTestCase(VectorClockIntegrationTests))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
