# Distributed Chat System with Total-Order Multicast

A robust distributed chat application implementing consensus and reliable messaging protocols for a fault-tolerant, multi-node cluster.

## Overview

This system enables multiple nodes to participate in a shared chat where all messages are delivered in **identical order across all nodes**. This is achieved through a combination of:
- **Bully Election Algorithm** for leader election and failure recovery
- **Reliable Total-Order Multicast (ATOM)** for message ordering
- **Heartbeat-based Failure Detection** for automatic failover
- **Vector Clocks** for causal ordering semantics

## Architecture

### Components

**Core Node (src/node.py)**
- Implements all consensus and multicast protocols
- Manages distributed state (membership, sequences, vector clocks)
- Coordinates message ordering through the elected leader

**Network Layer (src/net.py)**
- UDP multicast for group message broadcasts
- UDP unicast for peer-to-peer communications
- JSON serialization for cross-platform compatibility

**Protocol Messages (src/protocol.py)**
- `DISCOVERY` - New nodes discover cluster membership
- `HEARTBEAT` - Liveness detection and sequence sync
- `MEMBERSHIP` - Leader broadcasts cluster membership changes
- `ELECTION` / `ELECTION_OK` - Bully algorithm coordination
- `COORDINATOR` - Announces new leader
- `PROPOSE` - Request message ordering from leader
- `ORDERED` - Leader broadcasts ordered message to all
- `DELIVER_ACK` - Acknowledge receipt of ordered message

**User Interface (src/ui.py)**
- Non-blocking terminal chat display
- Real-time message rendering with node IDs and sequence numbers

## Key Algorithms

### 1. Bully Election
When the leader fails (7-second timeout):
1. All followers simultaneously detect timeout and initiate election
2. Node sends `ELECTION` to all higher-ID peers
3. Higher-ID nodes respond with `ELECTION_OK` and start their own election
4. This cascades upward until the highest-ID node becomes leader
5. New leader broadcasts `COORDINATOR` message to all followers

**Bully Invariant**: Only the highest-ID node alive becomes leader → guaranteed liveness.

### 2. Reliable Total-Order Multicast (ATOM)
Message ordering through sequencer (leader):
1. Followers send `PROPOSE` to leader when user types a message
2. Leader assigns monotonically increasing sequence number (e.g., seq=1, seq=2, seq=3...)
3. Leader broadcasts `ORDERED` message via multicast + unicast (dual redundancy)
4. All nodes receive `ORDERED`, store in holdback queue, and deliver in sequence order
5. Followers send `DELIVER_ACK` back to leader
6. Leader retransmits via unicast if no ACK within 0.8 seconds (reliability)

**Result**: All nodes display messages in identical order despite failures.

### 3. Heartbeat-Based Failure Detection
**Leaders**:
- Receive heartbeats from followers every 2 seconds
- Extract `max_seq_delivered` from each heartbeat
- Detect dead nodes (no heartbeat for 7 seconds) and remove from cluster
- Broadcast updated membership

**Followers**:
- Send heartbeats to leader with current sequence delivery progress
- Monitor leader liveness via `last_seen` timestamp (ANY message counts)
- Trigger election if leader silent for 7 seconds

### 4. Vector Clocks
- Each node maintains vector clock: `[node_4:5, node_5:3, node_9:7]`
- Incremented when ordering a message
- Included in `ORDERED` messages for causal consistency
- Enables detection of message causality relationships

## How to Run

### Prerequisites
```bash
python3 (3.8+)
```

### Start Cluster
```bash
# Terminal 1 - Node 4
python3 main.py --id 4

# Terminal 2 - Node 5
python3 main.py --id 5

# Terminal 3 - Node 9
python3 main.py --id 9
```

### Send Messages
```
Node 4> Hello from node 4
Node 5> Response from node 5
Node 9> Another message
```

All nodes display in identical order with sequence numbers:
```
[seq=1] Node 4: Hello from node 4
[seq=2] Node 5: Response from node 5
[seq=3] Node 9: Another message
```

### Test Failure Recovery
Kill the leader (Ctrl+C), observe:
1. Followers detect timeout (~7 seconds)
2. Election cascade begins
3. Highest-ID node becomes new leader
4. System resumes accepting messages with continuous sequence numbers

## Configuration

**src/config.py** - Tunable parameters:
```python
HEARTBEAT_INTERVAL_SEC = 2.0        # How often followers send heartbeats
FAILURE_TIMEOUT_SEC = 7.0            # Time before marking node as dead
ELECTION_TIMEOUT_SEC = 2.0           # Time to wait for ELECTION_OK
ACK_TIMEOUT_SEC = 0.8                # Time before retransmitting ORDERED
```

## Logging

Each node generates `node_X.log` with:
- **INFO**: Important state changes (leader election, membership updates)
- **DEBUG**: Protocol-level operations (message sends/receives, sequence tracking)
- **WARNING**: Election events, timeouts, cascades
- **ERROR**: Network failures, delivery failures

### Log Examples
```
2026-02-12 10:25:15 - ELECTION INITIATED BY NODE 4: Reason=LEADER_TIMEOUT, last_seen_age=7.05s
2026-02-12 10:25:15 - ELECTION CALCULATION: node_id=4, all_members=['4','5','9'], higher_ids=[5,9]
2026-02-12 10:25:15 - ELECTION SENT: from_node=4, to_higher_nodes=[5,9], count=2
2026-02-12 10:25:15 - ELECTION MESSAGE SENT: from=4, to=5, term=0
2026-02-12 10:25:15 - CASCADING_ELECTION: node=5, triggered_by=4, reason=received_lower_election
2026-02-12 10:25:17 - ELECTION COMPLETED: node=9, result=ELECTED_LEADER, term=0
```

## Message Flow Example

**Scenario**: Nodes 4, 5, 9 alive. Node 4 types message "hello"

```
Timeline:
T=0     Node 4 sends PROPOSE to leader (node 9)
T+0.01  Leader (9) receives PROPOSE, assigns seq=42, broadcasts ORDERED (multicast)
T+0.02  Nodes 4,5,9 receive ORDERED via multicast
T+0.02  Nodes 4,5,9 send DELIVER_ACK to leader
T+0.03  Leader receives all 3 ACKs, removes (seq=42) from pending list
Result: Message delivered to all in order [seq=42] Node 4: "hello"

If node 5's DELIVER_ACK is lost:
T+0.8   Leader timeout triggers → retransmits ORDERED via unicast to node 5
T+0.81  Node 5 receives unicast ORDERED, sends DELIVER_ACK
T+0.82  Leader receives ACK, cleanup
```

## Design Decisions

**Why Total Order?**
- Users expect consistent conversation flow across all nodes
- Simpler semantics than causal or FIFO ordering
- Leader-based sequencer is straightforward and reliable

**Why Bully Algorithm?**
- Simple, deterministic leader selection by node ID
- No configuration needed (no explicit coordinator roles)
- Automatic recovery from leader failures

**Why Dual Unicast + Multicast?**
- Multicast for efficient normal-case delivery
- Unicast retransmit for reliability if multicast fails
- Ensures delivery despite network losses

**Why Vector Clocks?**
- Optional feature for advanced causal analysis
- Enables detection of concurrent vs. causally-dependent messages
- Preserved in logs and messages for debugging

## Performance Characteristics

| Metric | Value |
|--------|-------|
| Message delivery latency | ~10-50ms (network dependent) |
| Leader election time | ~2-7 seconds (depends on failure detection timeout) |
| Max cluster size | Limited by multicast group size (~100+ nodes practical) |
| Sequence number growth | O(messages) - monotonically increasing |
| Memory per node | O(|cluster members| + outstanding messages) |

## Known Limitations

1. **Single Leader Bottleneck**: All write operations routed through leader
2. **No Byzantine Tolerance**: Assumes nodes don't send malicious messages
3. **No Persistence**: Messages not persisted to disk (lost on shutdown)
4. **No Encryption**: Communications sent in plaintext
5. **Limited to LAN**: Multicast doesn't work across WAN

## Future Enhancements

- [ ] Persistent log with crash recovery
- [ ] Raft or Paxos for Byzantine tolerance
- [ ] Sharding for horizontal scalability
- [ ] TLS encryption for network communications
- [ ] Web UI with WebSocket support
- [ ] Message history queries with sequence range

## Testing Scenario

**Test Failure Recovery**:
```
1. Start 3 nodes (4, 5, 9)
2. Type message: "Test message 1"
3. Kill node 9 (current leader): Ctrl+C
4. Wait ~7 seconds for election
5. Observe: Node 5 becomes new leader
6. Type message: "Test message 2"
7. Verify: Both nodes display seq=1, seq=2 identically
```

## References

- Bully Algorithm: Garcia-Molina, "Elections in a Distributed Computing System"
- Total-Order Multicast: Birman & Joseph, "Exploiting Virtual Synchrony in Distributed Systems"
- Vector Clocks: Lamport, "Time, Clocks, and the Ordering of Events"

---

**Author**: Vignesh Naidu  
**Date**: February 2026  
**Status**: Production-ready with comprehensive logging and failure detection
