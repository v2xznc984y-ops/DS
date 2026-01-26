# Distributed System - Architecture & Design

## Overview
A distributed system implementation with leader election, dynamic discovery, and multicast-based communication.

## System Components

### 1. Configuration (`src/config.py`)
- **MCAST_GRP**: Multicast group address (239.1.2.3)
- **MCAST_PORT**: Multicast port (50000)
- **BASE_PORT**: Base port for unicast (51000)
- **Discovery**: Retries (5), timeout (1.0s)
- **Heartbeat**: Interval (2.0s)
- **Failure Detection**: Timeout (7.0s)
- **Election**: Timeout (2.0s)
- **ACK**: Timeout (0.8s), Retries (5)

### 2. Protocol (`src/protocol.py`)
Helper functions and message types:
- `now()`: Returns current timestamp
- `msg_id()`: Generates unique message IDs (UUID4)
- `make_msg(mtype, from_id, term, payload)`: Creates message dictionaries
- Message Types:
  - `DISCOVERY`: Node discovery multicast
  - `DISCOVERY_REPLY`: Leader responds with membership

### 3. Network (`src/net.py`)
UDP communication utilities:
- `send_json(sock, addr, msg)`: Send JSON over UDP
- `recv_json(sock, bufsize)`: Receive JSON from UDP
- `make_unicast_socket()`: Create unicast socket
- `make_multicast_listener_socket()`: Create and join multicast group

### 4. Node (`src/node.py`)
Core node class with:
- **Fields**: node_id, term, is_leader, leader_id, leader_addr, members, last_seen
- **Sockets**: Unicast and multicast listeners
- **Threads**: Multicast and unicast listener daemon threads
- **Message Handler**: `on_message(msg, addr, channel)` - overrideable callback

### 5. Main Runner (`src/main.py`)
Entry point:
- Parses `--id` argument
- Starts Node(id)
- Runs until Ctrl+C

## Workflow

### Phase 1: Dynamic Discovery ✅ COMPLETE
Nodes discover each other and join cluster. New nodes added to membership automatically.

### Phase 2: Heartbeat & Failure Detection ✅ COMPLETE
- **2.1**: Followers send heartbeat every 2.0s to leader
- **2.2**: Leader tracks `last_seen` on all messages  
- **2.3**: Leader detects failed nodes every 1s (7.0s timeout)
- **2.4**: Leader broadcasts membership updates when topology changes

**How it works**:
1. Followers → send HEARTBEAT to leader every 2.0s
2. Leader → updates `last_seen[follower]` on any message
3. Leader → checks if `now - last_seen[peer] > 7.0s` and removes dead nodes
4. Leader → broadcasts MEMBERSHIP with updated cluster to all peers
5. Followers → receive MEMBERSHIP and update their view

**Status**: Working! Dead nodes detected and removed, membership synchronized across cluster.

## Phase 3: Leader Election ✅ COMPLETE
- **3.1**: Bully algorithm implementation with ELECTION messages
- **3.2**: Leader sends COORDINATOR announcement on election win
- **3.3**: Term-based versioning to prevent stale leaders
- **3.4**: Graceful failover when leader dies (election triggered, new leader elected)

**How it works**:
1. Node detects no heartbeat or notices election_in_progress flag
2. Node sends ELECTION to all higher-priority nodes
3. Nodes that receive ELECTION respond with ELECTION_OK
4. If no ELECTION_OK received, node becomes leader → broadcasts COORDINATOR
5. All nodes recognize new leader by COORDINATOR announcement

**Status**: Working! Tested 3-node cluster with leader death - new leader elected within ~3s.

## Phase 4: Reliable Total Order Multicast 🔄 IN PROGRESS
- **4.1**: State variables (seq numbers, holdback queue, pending ACKs) ✅
- **4.2**: User input thread and PROPOSE routing ✅
- **4.3**: Leader message ordering with sequence assignment ✅
- **4.4**: ORDERED message delivery with holdback queue - TODO
- **4.5**: DELIVER_ACK acknowledgement handling - TODO
- **4.6**: Retransmission for lost ORDERED messages - TODO

**How it works**:
1. User/follower proposes message → PROPOSE to leader
2. Leader orders: assigns seq, broadcasts ORDERED to all
3. Followers holdback ORDERED until seq matches next_seq_to_deliver
4. Followers deliver messages in-order, send DELIVER_ACK
5. Leader retransmits ORDERED if ACKs missing

## Current Status

✅ **Phase 1**: Dynamic Discovery - COMPLETE
✅ **Phase 2**: Heartbeat & Failure Detection - COMPLETE
✅ **Phase 3**: Leader Election (Bully) - COMPLETE
🔄 **Phase 4**: Reliable Total Order Multicast - 50% (steps 4.1-4.3 done)

## Known Bugs
See [BUGS.md](BUGS.md) for detailed issue tracking

## Running

```bash
# Start Node 1
python3 src/main.py --id 1

# Start Node 2 (in another terminal)
python3 src/main.py --id 2
```

## File Structure
```
DS/
├── src/
│   ├── __init__.py
│   ├── config.py       (Configuration constants)
│   ├── protocol.py     (Message helpers & types)
│   ├── net.py          (UDP utilities)
│   ├── node.py         (Node class)
│   └── main.py         (Entry point)
├── css                 (Placeholder)
├── README.md           (This file - Architecture)
└── BUGS.md             (Bugs & Errors Log)
```
