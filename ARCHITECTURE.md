# Layered Architecture Documentation

## Overview

The distributed system now uses a **three-layer architecture** that separates concerns and provides clean abstractions:

```
┌─────────────────────────────────────────┐
│     APPLICATION LAYER                   │
│  (User Interface & Business Logic)      │
│  - Message sending/receiving            │
│  - Event callbacks                      │
│  - Cluster status queries               │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│     MIDDLEWARE LAYER                    │
│  (Ordering & Coordination)              │
│  - Message ordering guarantees          │
│  - Membership management                │
│  - Delivery coordination                │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│     CORE LAYER (Node)                   │
│  (Distributed Consensus & Networking)   │
│  - Bully algorithm elections            │
│  - Failure detection                    │
│  - Multicast/Unicast messaging          │
│  - Leader election coordination         │
└─────────────────────────────────────────┘
```

## Layer Descriptions

### 1. Core Layer (Node)
**File:** `src/node.py`

The foundation of the system, handles:
- **Consensus Algorithm:** Bully election with UUID-based ordering
- **Failure Detection:** Heartbeat monitoring and leader failure detection
- **Network Communication:** UDP multicast and unicast messaging
- **Message Ordering:** Total order FIFO with sequence numbers
- **State Management:** Leader/follower roles, membership tracking

**Key Methods:**
- `start_election()` - Initiate leader election
- `_become_leader()` - Transition to leader state
- `propose_message(msg)` - Propose message for ordering
- `on_message(msg, addr, channel)` - Handle incoming messages

---

### 2. Middleware Layer (MiddlewareLayer)
**File:** `src/middleware_layer.py`

Bridges the core system with applications, provides:
- **Message Ordering:** Ensures reliable total-order delivery
- **Membership Abstraction:** Clean API for cluster member queries
- **Health & Status:** Node role, cluster statistics
- **Election Control:** Wait for stability, trigger elections (testing)
- **Message History:** Access to recent ordered messages

**Key Methods:**
```python
# Sending and receiving
send_message(message)
get_next_delivered_message(timeout)

# Cluster info
get_cluster_members()
get_leader()
is_leader()
get_node_role()  # Returns: "LEADER", "FOLLOWER", "CANDIDATE"

# Synchronization
wait_for_leader_election(timeout)
wait_for_cluster_stable(min_members, timeout)

# Statistics
get_stats()
get_message_history(limit)
```

**Example Usage:**
```python
# Wait for cluster to stabilize
middleware.wait_for_cluster_stable(min_members=3, timeout=10.0)

# Send a message
middleware.send_message({"type": "CHAT", "content": "Hello"})

# Get next delivered message
msg = middleware.get_next_delivered_message(timeout=1.0)
```

---

### 3. Application Layer (ApplicationLayer)
**File:** `src/application_layer.py`

High-level interface for user applications:
- **Message API:** Send and receive messages easily
- **Event Callbacks:** React to cluster events (node joined, leader elected, etc.)
- **Status Monitoring:** Query node and cluster status
- **Message Queue:** Asynchronous message delivery with timeouts

**Key Methods:**
```python
# Sending and receiving
send_message(text, message_type="CHAT") -> bool
receive_message(timeout=None) -> dict

# Callbacks
register_callback(event_type, callback)
# event_types: "on_message_received", "on_node_joined", "on_node_left", 
#             "on_leader_elected", "on_leader_changed"

# Status
get_cluster_members()
get_leader()
is_leader()
get_node_status()
```

**Example Usage:**
```python
def on_message(msg):
    print(f"Received: {msg['content']} from {msg['sender_id']}")

app.register_callback("on_message_received", on_message)

app.send_message("Hello, cluster!")

status = app.get_node_status()
print(f"I am the leader: {app.is_leader()}")
```

---

## Data Flow

### Sending a Message
```
Application Layer
    │ app.send_message("Hello")
    ▼
Middleware Layer
    │ middleware.send_message(wrapped_msg)
    ▼
Core Layer
    │ core_node.propose_message(msg)
    ▼
    [Message ordering & multicast]
    ▼
All Nodes Receive in Total Order
```

### Receiving a Message
```
Core Layer
    │ ORDERED message received
    ▼
Middleware Layer
    │ notify_message_delivered(msg)
    ▼
Application Layer
    │ app.notify_message_received(msg)
    ▼
Application
    │ receive_message() or callback invoked
```

### Leader Election Flow
```
Core Layer
    │ Leader failure detected
    │ start_election() initiated
    ▼
    [Bully algorithm runs]
    ▼
    New leader elected
    │ COORDINATOR broadcast
    ▼
Middleware Layer
    │ get_leader() updated
    ▼
Application Layer
    │ on_leader_changed callback fired
    ▼
Application
    │ React to new leadership
```

---

## Initialization Sequence

```python
# 1. Create core node
core_node = Node(node_id)

# 2. Create middleware
middleware = MiddlewareLayer(core_node)

# 3. Create application layer
app = ApplicationLayer(middleware)

# 4. Connect references
core_node.application_layer = app
core_node.middleware_layer = middleware

# 5. Start layers in order
core_node.start()              # Core consensus & networking
middleware.start()             # Message ordering
app.initialize(node_id)        # Initialize app
app.start()                    # Start event processing

# 6. Application can now use the API
middleware.wait_for_leader_election(timeout=5.0)
app.send_message("Hello")
```

---

## Separation of Concerns

| Concern | Layer | Responsibility |
|---------|-------|-----------------|
| User Interface | Application | Display, input, callbacks |
| Message Ordering | Middleware | Total order guarantee, delivery |
| Consensus | Core | Election, leader coordination |
| Networking | Core | UDP, multicast, addressing |

---

## Benefits of Layered Architecture

1. **Modularity:** Each layer has clear responsibilities
2. **Testability:** Layers can be tested independently
3. **Maintainability:** Changes to one layer don't affect others
4. **Extensibility:** Easy to add new middleware features (e.g., encryption, compression)
5. **Clarity:** Clear data flow from application to network and back
6. **Reusability:** Core layer can support multiple applications/middleware implementations

---

## Event Types

Applications can register callbacks for these events:

- **`on_message_received`** - A message was delivered in order
  ```python
  def handler(msg):
      print(f"Message: {msg['content']} from {msg['sender_id']}")
  app.register_callback("on_message_received", handler)
  ```

- **`on_node_joined`** - A new node joined the cluster
  ```python
  def handler(event):
      print(f"Node joined: {event['node_id']}")
  app.register_callback("on_node_joined", handler)
  ```

- **`on_node_left`** - A node left or failed
  ```python
  def handler(event):
      print(f"Node left: {event['node_id']}")
  app.register_callback("on_node_left", handler)
  ```

- **`on_leader_elected`** - A new leader was elected
  ```python
  def handler(event):
      print(f"New leader: {event['leader_id']}")
  app.register_callback("on_leader_elected", handler)
  ```

- **`on_leader_changed`** - Leadership changed
  ```python
  def handler(event):
      print(f"Old leader: {event.get('old_leader')}, New: {event.get('new_leader')}")
  app.register_callback("on_leader_changed", handler)
  ```

---

## Usage Patterns

### Pattern 1: Simple Message Broadcasting
```python
# Any node can send
app.send_message("Important announcement")

# All nodes receive in order
msg = app.receive_message(timeout=5.0)
print(f"Received: {msg['content']}")
```

### Pattern 2: Leader-Only Operations
```python
if app.is_leader():
    # Perform leader-only logic
    app.send_message("I am the leader")
```

### Pattern 3: Monitoring Cluster Health
```python
def on_status_check():
    status = app.get_node_status()
    members = app.get_cluster_members()
    print(f"Cluster size: {len(members)}, Leader: {app.get_leader()}")

# Run periodically
```

### Pattern 4: React to Events
```python
def on_node_joined(event):
    app.send_message(f"Welcome {event['node_id']}!")

app.register_callback("on_node_joined", on_node_joined)
```

---

## Performance Characteristics

- **Message Delivery:** Total order in milliseconds
- **Leader Election:** ~7-10 seconds (includes FAILURE_TIMEOUT_SEC)
- **Failure Detection:** ~7 seconds for leader failure
- **Heartbeat Interval:** 2 seconds
- **Cluster Stabilization:** ~2 seconds after leader transition

---

## Future Extensions

Potential middleware enhancements:

1. **Persistence Layer:** Store ordered messages to disk
2. **Compression:** Compress messages before transmission
3. **Encryption:** Encrypt messages in transit
4. **Rate Limiting:** Throttle message rate
5. **Message Filtering:** Route messages based on type
6. **Transactions:** Atomic multi-message operations
7. **Snapshots:** State snapshots for fast node recovery

---

## Troubleshooting

### Messages not being received
- Check if middleware layer is started
- Verify cluster has a leader: `middleware.wait_for_leader_election()`
- Check network connectivity

### Application callbacks not firing
- Ensure callback is registered before events occur
- Verify callback function doesn't raise exceptions
- Check if events are being sent from core layer

### Leader election taking too long
- Normal: Takes ~7 seconds (FAILURE_TIMEOUT_SEC)
- Check for network partitions
- Verify all nodes are running

