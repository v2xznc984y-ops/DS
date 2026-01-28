# Reliable Message Delivery Implementation

## Overview
Implemented a robust retry mechanism to handle message loss when the leader crashes during message ordering. This prevents messages from being lost if a leader failure occurs immediately after a follower sends a PROPOSE.

## Architecture

### Data Structures
- **`pending_proposes`**: Dictionary tracking all PROPOSE messages awaiting ordering
  - Key: `msg_id` (unique message identifier)
  - Value: Dict with `payload`, `tries`, `last_try`, `sender_id`
  - Location: Initialized at line 80 in `__init__()`

### Configuration
- **`MAX_PROPOSE_RETRIES`**: 10 maximum retry attempts (line 75)
- **`PROPOSE_RETRY_TIMEOUT`**: 2.0 seconds between retries (line 76)

## Implementation Details

### 1. Message Tracking (_cli_input_loop, line 520)
When a follower sends a PROPOSE message:
```python
self.pending_proposes[mid] = {
    "payload": payload,
    "tries": 0,
    "last_try": time.time(),
    "sender_id": self.node_id
}
```

**Timing**: Message is added to tracking immediately after being sent, not waiting for any confirmation.

### 2. Reliable Retransmission (_reliable_propose_retransmit_loop, line 668)
Background thread runs every 2.0 seconds:
- Checks all pending PROPOSEs
- Resends if PROPOSE_RETRY_TIMEOUT has elapsed since last attempt
- Increments try counter and updates last_try timestamp
- Stops retrying when MAX_PROPOSE_RETRIES is exceeded

**Error Handling**:
- Gracefully continues if `self.leader_addr` is None
- Catches exceptions on send failure and continues retrying

### 3. Cleanup on Ordering (line 1056)
When ORDERED message is received:
```python
if msg_id in self.pending_proposes:
    del self.pending_proposes[msg_id]
```

**Rationale**: Once the message is ordered and delivered, no more retransmissions are needed.

## Failure Scenarios Handled

### Scenario 1: Leader Crashes Between PROPOSE and ORDERED
1. Follower sends PROPOSE → Added to `pending_proposes`
2. Leader crashes before ordering
3. New leader elected
4. Retry loop resends PROPOSE to new leader (every 2.0s)
5. Eventually ordered by new leader
6. ORDERED received → Removed from `pending_proposes`

### Scenario 2: Network Packet Loss
1. Initial PROPOSE is lost in network
2. Retry loop detects timeout, resends
3. Leader eventually receives retry attempt
4. Message gets ordered

### Scenario 3: Cascading Leader Crashes
1. Follower sends PROPOSE
2. Leader 1 crashes → New retry to Leader 2
3. Leader 2 crashes → New retry to Leader 3
4. Continues until message is ordered (or max retries exceeded)

## Testing Strategy

### Unit Test: Basic Retry Logic
```python
# Send message, verify it's in pending_proposes
# Wait 2+ seconds
# Verify retry count incremented
# Receive ORDERED
# Verify removed from pending_proposes
```

### Integration Test: Leader Failure During Ordering
1. Start 3 nodes (Node 1 is leader)
2. Node 2 sends message via PROPOSE
3. Kill Node 1 leader process
4. Node 3 becomes new leader
5. Verify Node 2's message eventually gets ordered by Node 3
6. Verify all nodes display the message in order

### Stress Test: Multiple Concurrent Proposals + Failures
1. Multiple nodes each send 5 messages
2. Randomly kill leader while messages in flight
3. Verify all messages eventually ordered (none lost)
4. Verify correct total order across all nodes

## Configuration Tuning

### For High-Latency Networks
- Increase `PROPOSE_RETRY_TIMEOUT` to 5.0s
- Increase `MAX_PROPOSE_RETRIES` to 20

### For Low-Latency Networks
- Decrease `PROPOSE_RETRY_TIMEOUT` to 1.0s
- Keep `MAX_PROPOSE_RETRIES` at 10 or higher

## Code Locations

| Component | Location |
|-----------|----------|
| Data structure init | Line 80 `self.pending_proposes = {}` |
| Config params | Lines 75-76 `MAX_PROPOSE_RETRIES`, `PROPOSE_RETRY_TIMEOUT` |
| Tracking on send | Lines 520-525 in `_cli_input_loop()` |
| Retry thread start | Line 156 in `_start_listeners()` |
| Retry loop logic | Lines 668-705 in `_reliable_propose_retransmit_loop()` |
| Cleanup on order | Lines 1056-1058 in ORDERED handler |

## Performance Impact

- **Memory**: O(P) where P = number of pending PROPOSEs (typically < 100)
- **CPU**: Minimal - one background thread sleeping 2.0s between checks
- **Network**: Adds retransmissions only when leader is unresponsive

## Future Enhancements

1. **Exponential Backoff**: Currently fixed 2.0s interval, could use exponential backoff
2. **ACK-based Confirmation**: Could send ACK from leader for each PROPOSE received
3. **Message Priority**: Could prioritize retry of older messages first
4. **Adaptive Timeouts**: Could detect network conditions and adjust retry interval

## References

- **Pattern**: Similar to application-layer retry mechanism (inspired by user's `_reliable_send_loop()`)
- **Related**: Works in conjunction with leader's `_retransmit_loop()` for ORDERED message reliability
- **Failure Detector**: Integrated with heartbeat-based failure detection for leader changes
