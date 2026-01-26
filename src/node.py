import threading
import time
import sys
import os
import socket

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import BASE_PORT, MCAST_GRP, MCAST_PORT, DISCOVERY_RETRIES, DISCOVERY_TIMEOUT_SEC, HEARTBEAT_INTERVAL_SEC, FAILURE_TIMEOUT_SEC, ELECTION_TIMEOUT_SEC, ACK_TIMEOUT_SEC, ACK_RETRIES
from src.protocol import make_msg, msg_id, DISCOVERY, DISCOVERY_REPLY, HEARTBEAT, MEMBERSHIP, ELECTION, ELECTION_OK, COORDINATOR, PROPOSE, ORDERED, DELIVER_ACK
from src.net import make_unicast_socket, make_multicast_listener_socket, recv_json, send_json


class Node:
    """Distributed system node with multicast and unicast listeners."""
    
    def __init__(self, node_id):
        """
        Initialize a node.
        
        Args:
            node_id: Unique integer identifier for this node
        """
        self.node_id = node_id
        self.term = 0
        self.is_leader = False
        self.leader_id = None
        self.leader_addr = None
        self.members = {}  # node_id -> addr mapping
        self.last_seen = {}  # node_id -> timestamp mapping
        
        # For discovery synchronization
        self.discovery_reply = None
        
        # Election state
        self.election_in_progress = False
        self.awaiting_coordinator = False
        self.last_election_start_ts = None
        
        # Track last membership update from leader (for followers)
        self.last_membership_update = time.time()
        
        # Reliable Total Order Multicast (ATOM) - all nodes
        self.next_seq_to_deliver = 1  # Next sequence number to deliver
        self.holdback = {}  # seq -> message (out-of-order messages waiting to be delivered)
        
        # ATOM - leader only
        self.next_seq_to_assign = 1  # Next sequence number to assign
        self.pending_acks = {}  # (seq, msg_id) -> set of peer_ids missing ACK
        self.ack_retry_count = {}  # (seq, msg_id) -> retry count
        
        # Create sockets
        self.unicast_sock = make_unicast_socket("0.0.0.0", BASE_PORT + node_id)
        self.multicast_sock = make_multicast_listener_socket(MCAST_GRP, MCAST_PORT)
        
        # Start listener threads
        self._start_listeners()
    
    def _start_listeners(self):
        """Start multicast and unicast listener daemon threads."""
        multicast_thread = threading.Thread(
            target=self._multicast_listener,
            daemon=True,
            name=f"Node-{self.node_id}-Multicast"
        )
        multicast_thread.start()
        
        unicast_thread = threading.Thread(
            target=self._unicast_listener,
            daemon=True,
            name=f"Node-{self.node_id}-Unicast"
        )
        unicast_thread.start()
        
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"Node-{self.node_id}-Heartbeat"
        )
        heartbeat_thread.start()
        
        # Start failure detector thread
        failure_detector_thread = threading.Thread(
            target=self._failure_detector_loop,
            daemon=True,
            name=f"Node-{self.node_id}-FailureDetector"
        )
        failure_detector_thread.start()
        
        # Start leader alive check thread (for followers)
        leader_check_thread = threading.Thread(
            target=self._check_leader_alive,
            daemon=True,
            name=f"Node-{self.node_id}-LeaderCheck"
        )
        leader_check_thread.start()
        
        # Start periodic membership broadcast (for leader)
        membership_broadcast_thread = threading.Thread(
            target=self._periodic_membership_broadcast,
            daemon=True,
            name=f"Node-{self.node_id}-MembershipBroadcast"
        )
        membership_broadcast_thread.start()
        
        # Start CLI input thread for user messages
        cli_thread = threading.Thread(
            target=self._cli_input_loop,
            daemon=True,
            name=f"Node-{self.node_id}-CLI"
        )
        cli_thread.start()
        
        # Retransmit loop thread (leader only, will check is_leader)
        retransmit_thread = threading.Thread(
            target=self._retransmit_loop,
            daemon=True,
            name=f"Node-{self.node_id}-Retransmit"
        )
        retransmit_thread.start()
    
    def start(self):
        """Start node: begin listeners, then perform discovery."""
        print(f"Node {self.node_id}: Starting listeners...")
        time.sleep(0.1)  # Brief delay to ensure threads start
        
        print(f"Node {self.node_id}: Beginning discovery...")
        self.startup_discovery()
        
        if self.is_leader:
            print(f"Node {self.node_id}: Elected as LEADER (term={self.term})")
        else:
            print(f"Node {self.node_id}: Joined cluster under leader {self.leader_id} at {self.leader_addr}")
    
    def startup_discovery(self):
        """
        Discover cluster: multicast DISCOVERY, wait for reply from leader.
        If no reply after retries, self-elect as leader.
        """
        # Create multicast socket for sending (separate from listener)
        mcast_send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # Get unicast port
        unicast_addr = self.unicast_sock.getsockname()
        unicast_port = unicast_addr[1]
        
        for attempt in range(DISCOVERY_RETRIES):
            # Multicast discovery message
            msg = make_msg(DISCOVERY, self.node_id, self.term, {"unicast_port": unicast_port})
            send_json(mcast_send_sock, (MCAST_GRP, MCAST_PORT), msg)
            print(f"Node {self.node_id}: Sent DISCOVERY (attempt {attempt + 1}/{DISCOVERY_RETRIES})")
            
            # Wait for reply from listener thread
            time.sleep(DISCOVERY_TIMEOUT_SEC)
            
            if self.discovery_reply:
                # Got a reply from listener thread
                reply, addr = self.discovery_reply
                payload = reply.get("payload", {})
                self.leader_id = payload.get("leader_id")
                self.leader_addr = addr
                self.term = payload.get("term", 0)
                self.members = payload.get("members", {})
                
                # Initialize last_seen to NOW for all members
                now = time.time()
                self.last_seen = {member_id: now for member_id in self.members.keys()}
                
                print(f"Node {self.node_id}: Received DISCOVERY_REPLY from leader {self.leader_id}")
                mcast_send_sock.close()
                return
        
        # No reply received - self-elect as leader
        self.is_leader = True
        self.leader_id = self.node_id
        self.leader_addr = self.unicast_sock.getsockname()
        self.term += 1
        self.members[self.node_id] = self.leader_addr
        self.last_seen[self.node_id] = time.time()
        mcast_send_sock.close()
        print(f"Node {self.node_id}: No leader found, self-electing as leader")
    
    def _multicast_listener(self):
        """Listen for multicast messages and dispatch to on_message."""
        while True:
            try:
                msg, addr = recv_json(self.multicast_sock)
                self.on_message(msg, addr, "multicast")
            except Exception as e:
                print(f"Node {self.node_id} multicast error: {e}")
    
    def _unicast_listener(self):
        """Listen for unicast messages and dispatch to on_message."""
        while True:
            try:
                msg, addr = recv_json(self.unicast_sock)
                self.on_message(msg, addr, "unicast")
            except Exception as e:
                print(f"Node {self.node_id} unicast error: {e}")
    
    def _heartbeat_loop(self):
        """Send heartbeats to leader periodically."""
        while True:
            try:
                time.sleep(HEARTBEAT_INTERVAL_SEC)
                
                # Only send heartbeat if not leader and leader exists
                if not self.is_leader and self.leader_addr:
                    msg = make_msg(HEARTBEAT, self.node_id, self.term)
                    send_json(self.unicast_sock, self.leader_addr, msg)
                    # Uncomment for debugging:
                    # print(f"Node {self.node_id}: Sent HEARTBEAT to leader at {self.leader_addr}")
            except Exception as e:
                print(f"Node {self.node_id} heartbeat error: {e}")
    
    def _failure_detector_loop(self):
        """Detect failed nodes by checking last_seen timestamps (leader only)."""
        while True:
            try:
                time.sleep(1.0)  # Check every 1 second
                
                # Only leader does failure detection
                if not self.is_leader:
                    continue
                
                now = time.time()
                failed_nodes = []
                
                # Check last_seen for all members except self
                for member_id in list(self.members.keys()):
                    # Convert member_id to int for comparison (it's a string from dict)
                    member_id_int = int(member_id) if isinstance(member_id, str) else member_id
                    
                    if member_id_int == self.node_id:
                        continue  # Skip self
                    
                    last_seen_time = self.last_seen.get(member_id, now)
                    if now - last_seen_time > FAILURE_TIMEOUT_SEC:
                        failed_nodes.append(member_id)
                
                # Remove failed nodes and broadcast
                for node_id in failed_nodes:
                    print(f"Node {self.node_id}: Detected failure of node {node_id} (no heartbeat for {FAILURE_TIMEOUT_SEC}s)")
                    if node_id in self.members:
                        del self.members[node_id]
                    if node_id in self.last_seen:
                        del self.last_seen[node_id]
                    
                    # Broadcast updated membership
                    self._broadcast_membership()
            
            except Exception as e:
                print(f"Node {self.node_id} failure detector error: {e}")
    
    def _broadcast_membership(self):
        """Broadcast membership update to all peers (leader only)."""
        if not self.is_leader:
            return
        
        payload = {
            "term": self.term,
            "members": self.members,
            "last_seen": self.last_seen
        }
        msg = make_msg(MEMBERSHIP, self.node_id, self.term, payload)
        
        # Send to all peers (skip those that fail)
        for member_id, addr in list(self.members.items()):
            if member_id == self.node_id:  # Don't send to self
                continue
            
            try:
                # Convert addr to tuple if it's a list (from JSON)
                if isinstance(addr, list):
                    addr = tuple(addr)
                send_json(self.unicast_sock, addr, msg)
            except Exception as e:
                # Don't print every error, just silently skip
                pass
    
    def higher_ids(self):
        """Return list of member IDs greater than this node's ID."""
        return [int(mid) for mid in self.members.keys() if int(mid) > self.node_id]
    
    def start_election(self):
        """Start bully election: send ELECTION to higher IDs, wait for response."""
        if self.election_in_progress:
            return  # Already running election
        
        self.election_in_progress = True
        self.awaiting_coordinator = False
        self.term += 1  # New epoch
        self.last_election_start_ts = time.time()
        
        print(f"Node {self.node_id}: Starting election (term={self.term})")
        
        # Get IDs of peers with higher node_id
        higher = self.higher_ids()
        
        if not higher:
            # No higher IDs - self becomes leader
            self._become_leader()
        else:
            # Send ELECTION to all higher IDs
            msg = make_msg(ELECTION, self.node_id, self.term)
            for peer_id in higher:
                if peer_id in self.members:
                    peer_addr = self.members[peer_id]
                    try:
                        send_json(self.unicast_sock, peer_addr, msg)
                        print(f"Node {self.node_id}: Sent ELECTION to higher peer {peer_id}")
                    except Exception as e:
                        print(f"Node {self.node_id}: Failed to send ELECTION to {peer_id}: {e}")
            
            # Wait for OK response
            self.awaiting_coordinator = True
            
            # Start timer for election timeout
            def election_timeout():
                time.sleep(ELECTION_TIMEOUT_SEC)
                
                # Check if we got OK response
                if self.awaiting_coordinator:
                    # No OK received - become leader
                    print(f"Node {self.node_id}: No OK received within {ELECTION_TIMEOUT_SEC}s, becoming leader")
                    self._become_leader()
            
            timeout_thread = threading.Thread(target=election_timeout, daemon=True)
            timeout_thread.start()
    
    def _check_leader_alive(self):
        """Followers check if leader is still alive (via MEMBERSHIP updates)."""
        while True:
            try:
                time.sleep(1.0)
                
                # Only followers check leader
                if self.is_leader or not self.leader_id:
                    continue
                
                # If haven't received MEMBERSHIP update from leader in FAILURE_TIMEOUT_SEC, assume dead
                now = time.time()
                if now - self.last_membership_update > FAILURE_TIMEOUT_SEC:
                    print(f"Node {self.node_id}: Leader {self.leader_id} appears to be dead (no update for {FAILURE_TIMEOUT_SEC}s)")
                    self.start_election()
                    break  # Stop checking once we start election
            
            except Exception as e:
                print(f"Node {self.node_id} leader check error: {e}")
    
    def _periodic_membership_broadcast(self):
        """Leader periodically broadcasts membership to keep followers alive."""
        while True:
            try:
                # Only leader does this
                if not self.is_leader:
                    time.sleep(1.0)
                    continue
                
                # Broadcast membership every HEARTBEAT_INTERVAL_SEC
                time.sleep(HEARTBEAT_INTERVAL_SEC)
                self._broadcast_membership()
            
            except Exception as e:
                print(f"Node {self.node_id} membership broadcast error: {e}")
    
    def _cli_input_loop(self):
        """Read user input from stdin and propose messages."""
        while True:
            try:
                # Read line from stdin
                text = input()
                if not text.strip():
                    continue
                
                # Create message with unique ID
                mid = msg_id()
                payload = {"text": text, "mid": mid}
                
                if self.is_leader:
                    # Leader directly orders the message
                    self._order_message(mid, payload)
                else:
                    # Follower sends PROPOSE to leader
                    propose_msg = make_msg(PROPOSE, self.node_id, self.term, payload)
                    try:
                        send_json(self.unicast_sock, self.leader_addr, propose_msg)
                        print(f"Node {self.node_id}: Proposed message '{text}'")
                    except Exception as e:
                        print(f"Node {self.node_id}: Failed to send PROPOSE to leader: {e}")
            
            except Exception as e:
                # Ignore input errors, continue loop
                pass
    
    def _order_message(self, msg_id, payload):
        """Leader orders a message with a sequence number."""
        # Assign sequence number
        seq = self.next_seq_to_assign
        self.next_seq_to_assign += 1
        
        # Create ORDERED message
        ordered_payload = {
            "seq": seq,
            "mid": msg_id,
            "payload": payload
        }
        ordered_msg = make_msg(ORDERED, self.node_id, self.term, ordered_payload)
        
        # Send to all members (including self for consistency)
        for member_id, addr in list(self.members.items()):
            try:
                # Convert addr to tuple if it's a list (from JSON)
                if isinstance(addr, list):
                    addr = tuple(addr)
                send_json(self.unicast_sock, addr, ordered_msg)
            except Exception as e:
                # Silently skip dead nodes
                pass
        
        # Initialize pending ACKs for this (seq, msg_id)
        # Track which members need to ACK (all except leader itself)
        pending_set = set()
        for member_id in self.members.keys():
            member_id_int = int(member_id) if isinstance(member_id, str) else member_id
            if member_id_int != self.node_id:
                pending_set.add(member_id)
        
        self.pending_acks[(seq, msg_id)] = pending_set
        print(f"Node {self.node_id}: Ordered message seq={seq}, mid={msg_id}, waiting for ACKs from {len(pending_set)} peers")
    
    def _retransmit_loop(self):
        """Periodically retransmit ORDERED messages to peers missing ACKs."""
        while True:
            time.sleep(ACK_TIMEOUT_SEC)
            
            # Only leader runs retransmission
            if not self.is_leader:
                continue
            
            # For each pending (seq, msg_id)
            for (seq, msg_id), missing_peers in list(self.pending_acks.items()):
                if not missing_peers:
                    # All peers have ACKed
                    continue
                
                # Increment retry count
                if (seq, msg_id) not in self.ack_retry_count:
                    self.ack_retry_count[(seq, msg_id)] = 0
                self.ack_retry_count[(seq, msg_id)] += 1
                
                # Check if exceeded max retries
                if self.ack_retry_count[(seq, msg_id)] > ACK_RETRIES:
                    # Treat missing peers as failed - remove from members
                    print(f"Node {self.node_id}: Max retries exceeded for (seq={seq}, mid={msg_id}). Removing {len(missing_peers)} peers from cluster")
                    
                    for peer_id in missing_peers:
                        if peer_id in self.members:
                            del self.members[peer_id]
                    
                    # Broadcast updated membership
                    self._broadcast_membership()
                    
                    # Clean up retry count
                    del self.ack_retry_count[(seq, msg_id)]
                    continue
                
                # Resend ORDERED to peers still missing ACK
                print(f"Node {self.node_id}: Retransmitting (seq={seq}, mid={msg_id}) to {len(missing_peers)} peers (retry #{self.ack_retry_count[(seq, msg_id)]})")
                
                # Reconstruct and resend ORDERED message to missing peers
                ordered_payload = {
                    "seq": seq,
                    "mid": msg_id,
                    "payload": {}  # Don't need original payload for retransmit
                }
                ordered_msg = make_msg(ORDERED, self.node_id, self.term, ordered_payload)
                
                for peer_id in missing_peers:
                    if peer_id in self.members:
                        addr = self.members[peer_id]
                        try:
                            if isinstance(addr, list):
                                addr = tuple(addr)
                            send_json(self.unicast_sock, addr, ordered_msg)
                        except Exception as e:
                            pass
    
    def _become_leader(self):
        """Declare self as leader and broadcast COORDINATOR."""
        self.is_leader = True
        self.leader_id = self.node_id
        self.leader_addr = self.unicast_sock.getsockname()
        self.election_in_progress = False
        self.awaiting_coordinator = False
        
        print(f"Node {self.node_id}: Elected as NEW LEADER (term={self.term})")
        
        # Initialize members dict if empty
        if not self.members:
            self.members[self.node_id] = self.leader_addr
        
        # Reset last_seen for all members to NOW (they haven't sent heartbeats yet in new term)
        now = time.time()
        self.last_seen = {member_id: now for member_id in self.members.keys()}
        
        # Remove dead nodes from members (nodes that haven't been seen in a while)
        dead_nodes = []
        for member_id in list(self.members.keys()):
            member_id_int = int(member_id) if isinstance(member_id, str) else member_id
            if member_id_int != self.node_id:
                # Check if this member was last seen more than FAILURE_TIMEOUT_SEC ago
                # (This catches nodes that were already suspected dead)
                last_seen_time = self.last_seen.get(member_id, now)
                if now - last_seen_time > FAILURE_TIMEOUT_SEC:
                    dead_nodes.append(member_id)
        
        # Remove dead nodes
        for node_id in dead_nodes:
            print(f"Node {self.node_id}: Removing dead node {node_id} from members")
            del self.members[node_id]
            if node_id in self.last_seen:
                del self.last_seen[node_id]
        
        # Broadcast COORDINATOR to all peers (skip dead ones)
        msg = make_msg(COORDINATOR, self.node_id, self.term, {"leader_id": self.node_id})
        for member_id, addr in list(self.members.items()):
            if member_id == self.node_id:
                continue
            
            try:
                # Convert addr to tuple if it's a list (from JSON)
                if isinstance(addr, list):
                    addr = tuple(addr)
                send_json(self.unicast_sock, addr, msg)
            except Exception as e:
                # Silently skip dead nodes
                pass
        
        # Broadcast updated membership
        self._broadcast_membership()
    
    def on_message(self, msg, addr, channel):
        """
        Handle incoming message.
        
        Args:
            msg: Message dictionary
            addr: Sender address (host, port)
            channel: "multicast" or "unicast"
        """
        msg_type = msg.get("type")
        from_id = msg.get("from_id")
        
        # Update last_seen on ANY message from a peer
        if from_id is not None:
            self.last_seen[from_id] = time.time()
        
        if msg_type == DISCOVERY:
            # A node is trying to discover the cluster
            if self.is_leader:
                # Add new member to cluster
                if from_id not in self.members:
                    unicast_port = msg.get("payload", {}).get("unicast_port")
                    if unicast_port:
                        member_addr = (addr[0], unicast_port)
                        self.members[from_id] = member_addr
                        self.last_seen[from_id] = time.time()
                        print(f"Node {self.node_id}: Added new member {from_id} to cluster")
                        # Broadcast updated membership
                        self._broadcast_membership()
                
                # Reply with cluster info
                payload = {
                    "term": self.term,
                    "leader_id": self.leader_id,
                    "members": self.members,
                    "last_seen": self.last_seen
                }
                reply = make_msg(DISCOVERY_REPLY, self.node_id, self.term, payload)
                
                # Send unicast reply to the discovering node
                unicast_port = msg.get("payload", {}).get("unicast_port")
                if unicast_port:
                    reply_addr = (addr[0], unicast_port)
                    send_json(self.unicast_sock, reply_addr, reply)
                    print(f"Node {self.node_id}: Responded to DISCOVERY from node {from_id} at {reply_addr}")
        
        elif msg_type == DISCOVERY_REPLY:
            # Store reply for startup_discovery to process
            self.discovery_reply = (msg, addr)
            print(f"Node {self.node_id}: Listener got DISCOVERY_REPLY from {addr}")
        
        elif msg_type == HEARTBEAT:
            # Received heartbeat from a peer
            # last_seen already updated above
            # Leader can optionally respond, but just tracking is enough
            if self.is_leader:
                # Could send HEARTBEAT_ACK, but ignoring for simplicity
                pass
        
        elif msg_type == MEMBERSHIP:
            # Received membership update from leader
            payload = msg.get("payload", {})
            msg_term = payload.get("term", 0)
            
            # Ignore own messages or messages from non-leaders
            if from_id == self.node_id:
                return  # Ignore self messages
            
            # Only accept if term >= current term
            if msg_term >= self.term:
                self.term = msg_term
                self.members = payload.get("members", {})
                
                # Initialize last_seen to NOW for all members (don't use stale timestamps)
                now = time.time()
                self.last_seen = {member_id: now for member_id in self.members.keys()}
                self.last_membership_update = now
                
                print(f"Node {self.node_id}: Updated membership from leader (term={msg_term}): members={list(self.members.keys())}")
        
        elif msg_type == ELECTION:
            # Received election from lower ID peer
            sender_id = from_id
            if sender_id < self.node_id:
                # Reply with OK
                ok_msg = make_msg(ELECTION_OK, self.node_id, self.term)
                try:
                    send_json(self.unicast_sock, addr, ok_msg)
                    print(f"Node {self.node_id}: Received ELECTION from node {sender_id}, sent OK")
                except Exception as e:
                    print(f"Node {self.node_id}: Failed to send OK to {sender_id}: {e}")
                
                # Start own election if not already in progress
                if not self.election_in_progress:
                    self.start_election()
        
        elif msg_type == ELECTION_OK:
            # Received OK from higher ID peer
            # Set awaiting_coordinator and do not become leader
            self.awaiting_coordinator = True
            print(f"Node {self.node_id}: Received OK from node {from_id}, awaiting coordinator")
            
            # Reset the election timeout when we receive an OK
            # (the higher node is running, so it will elect)
            if hasattr(self, '_election_start_time'):
                self._election_start_time = time.time()
        
        elif msg_type == COORDINATOR:
            # Received coordinator announcement
            payload = msg.get("payload", {})
            coordinator_id = payload.get("leader_id", from_id)
            coordinator_term = msg.get("term", 0)
            
            # Update if newer term or valid coordinator
            if coordinator_term >= self.term:
                self.term = coordinator_term
                self.leader_id = coordinator_id
                self.is_leader = (coordinator_id == self.node_id)
                
                # Update leader address from members dict
                if coordinator_id in self.members:
                    self.leader_addr = self.members[coordinator_id]
                else:
                    self.leader_addr = addr
                
                self.election_in_progress = False
                self.awaiting_coordinator = False
                
                # Reset last membership update timer (leader just announced itself)
                self.last_membership_update = time.time()
                
                print(f"Node {self.node_id}: Received COORDINATOR - new leader is {coordinator_id} (term={coordinator_term})")
        
        elif msg_type == PROPOSE:
            # Received message proposal from peer (leader only)
            if self.is_leader:
                payload = msg.get("payload", {})
                msg_id = payload.get("mid")
                self._order_message(msg_id, payload)
            else:
                # Non-leader ignores PROPOSE
                pass
        
        elif msg_type == ORDERED:
            # Received ordered message from leader
            payload = msg.get("payload", {})
            seq = payload.get("seq")
            msg_id = payload.get("mid")
            msg_payload = payload.get("payload", {})
            
            # Store in holdback queue
            self.holdback[seq] = {"msg_id": msg_id, "payload": msg_payload}
            
            # Send ACK back to leader
            ack_msg = make_msg(DELIVER_ACK, self.node_id, self.term, {"seq": seq, "mid": msg_id})
            try:
                send_json(self.unicast_sock, self.leader_addr, ack_msg)
            except Exception as e:
                print(f"Node {self.node_id}: Failed to send DELIVER_ACK to leader: {e}")
            
            # Deliver all messages with seq == next_seq_to_deliver in order
            while self.next_seq_to_deliver in self.holdback:
                msg_to_deliver = self.holdback.pop(self.next_seq_to_deliver)
                delivered_text = msg_to_deliver.get("payload", {}).get("text", "")
                print(f"Node {self.node_id}: DELIVERED message seq={self.next_seq_to_deliver}: '{delivered_text}'")
                self.next_seq_to_deliver += 1
        
        elif msg_type == DELIVER_ACK:
            # Leader receives ACK from peer
            if self.is_leader:
                payload = msg.get("payload", {})
                seq = payload.get("seq")
                msg_id = payload.get("mid")
                
                # Remove this peer from pending_acks for this (seq, msg_id)
                key = (seq, msg_id)
                if key in self.pending_acks:
                    peer_id = str(from_id) if isinstance(from_id, int) else from_id
                    self.pending_acks[key].discard(peer_id)
                    
                    # If all peers have ACKed, remove entry
                    if not self.pending_acks[key]:
                        del self.pending_acks[key]
                        if key in self.ack_retry_count:
                            del self.ack_retry_count[key]
                        print(f"Node {self.node_id}: All peers ACKed (seq={seq}, mid={msg_id})")
