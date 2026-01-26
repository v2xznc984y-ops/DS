# Bugs & Errors Log

## Resolved Issues

### 1. Module Import Error (src/node.py)
**Error**: `ModuleNotFoundError: No module named 'src'`
**Cause**: Running Python file directly doesn't recognize `src` package without parent directory in path
**Solution**: Added `sys.path.insert(0, os.path.dirname(...))` to dynamically add parent directory

**Status**: ✅ FIXED

---

### 2. Relative Import with Direct Execution
**Error**: `ImportError: attempted relative import with no known parent package`
**Cause**: Relative imports (`.config`) don't work when running file directly
**Solution**: Changed to absolute imports (`src.config`) with dynamic path setup

**Status**: ✅ FIXED

---

### 3. Missing `__init__.py`
**Error**: `ModuleNotFoundError: No module named 'src'`
**Cause**: `src/` directory wasn't recognized as a Python package
**Solution**: Created `src/__init__.py` (empty file)

**Status**: ✅ FIXED

---

## Known Issues

### 4. Both Nodes Self-Electing as Leader (Race Condition)
**Error**: When Node 1 and Node 2 run together, Node 2 wouldn't discover Node 1 as leader
**Cause**: Race condition between listener thread and startup_discovery() competing for socket reads
**Why**: startup_discovery() was blocking on socket.recv() with timeout, but listener thread was also reading from same socket
**Solution**: 
  - Listener thread captures DISCOVERY_REPLY and stores in `self.discovery_reply`
  - startup_discovery() polls `self.discovery_reply` instead of blocking on socket
  - No more race condition between threads

**Status**: ✅ FIXED

---

### 5. Multicast Port Already in Use
**Error**: `OSError: [Errno 48] Address already in use` when starting second node
**Cause**: Multiple nodes trying to bind to same multicast port (50000)
**Why**: `SO_REUSEADDR` alone doesn't allow multiple processes to bind to same UDP port
**Solution**: Added `SO_REUSEPORT` socket option to allow multiple listeners on same multicast port

**Status**: ✅ FIXED

---

### 6. Stale last_seen Values Causing False Failures
**Error**: All nodes detecting each other as failed immediately when Node 3 joins
**Cause**: New nodes received `last_seen` dict with old timestamps from leader
**Why**: last_seen values were created when cluster started, not reset when membership updated
**Solution**: When receiving MEMBERSHIP or DISCOVERY_REPLY, reset `last_seen` to NOW for all members

**Status**: ✅ FIXED

---

### 7. Followers Detecting Each Other as Failed
**Error**: Followers were marking each other as failed, even though only heartbeats go to leader
**Cause**: Followers were running failure detection on other followers
**Why**: Only leader receives heartbeats, so followers can never see them from each other
**Solution**: Only leader runs failure detector. Followers skip the check entirely.

**Status**: ✅ FIXED

---

### 8. Split-Brain Election (Multiple Leaders)
**Error**: When Node 1 dies, both Node 2 and Node 3 become leaders (different terms)
**Cause**: Both nodes start elections simultaneously without coordination
**Why**: Election timeout is too long, or both nodes declare themselves leaders
**Solution**: Need to ensure only ONE node wins election. Add election ID tracking or enforce bully algorithm strictly.

**Status**: 🔧 IN PROGRESS

---

### 9. Restarted Node Self-Elects Instead of Discovering
**Error**: Node 1 restarts and self-elects as leader even though Node 2/3 are leading
**Cause**: Discovery doesn't find the current leader (discovery replies come but discovery_reply isn't set?)
**Why**: Possible race condition in discovery or leader broadcast
**Solution**: May need to add leader broadcasts to DISCOVERY reply or longer discovery timeout

**Status**: 🔧 IN PROGRESS

---

## Testing Notes

- Node 1 starts successfully with `python3 src/main.py --id 1`
- Listener threads spawn and run as daemons
- Ctrl+C gracefully shuts down the node

