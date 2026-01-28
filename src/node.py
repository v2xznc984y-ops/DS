import threading
import time
import sys
import os
import socket

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import BASE_PORT, MCAST_GRP, MCAST_PORT, DISCOVERY_RETRIES, DISCOVERY_TIMEOUT_SEC, HEARTBEAT_INTERVAL_SEC, FAILURE_TIMEOUT_SEC, ELECTION_TIMEOUT_SEC, ACK_TIMEOUT_SEC, ACK_RETRIES
from src.protocol import make_msg, msg_id, DISCOVERY, DISCOVERY_REPLY, HEARTBEAT, MEMBERSHIP, ELECTION, ELECTION_OK, COORDINATOR, PROPOSE, ORDERED, DELIVER_ACK
from src.net import make_unicast_socket, make_multicast_listener_socket, make_multicast_sender_socket, recv_json, send_json
from src.ui import ChatUI
from src.vector_clock import VectorClock
from dataclasses import dataclass


@dataclass(frozen=True)
class ChatMessage:
    """Immutable chat message after ordering via total-order multicast."""
    sender_id: int
    text: str
    sequence_number: int
    vector_clock: dict  # Vector clock of sender when message was sent
    
    def __str__(self):
        return f"ChatMessage(sender={self.sender_id}, seq={self.sequence_number}, text='{self.text}', vc={self.vector_clock})"


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
        
        # Initialize chat UI (non-blocking terminal display)
        self.ui = ChatUI(node_id)
        
        # Vector clock for causal ordering (used in multicast messages)
        self.vc = VectorClock(node_id)
        
        # For discovery synchronization
        self.discovery_reply = None
        
        # Election state
        self.election_in_progress = False
        self.awaiting_coordinator = False
        # Track whether we received an OK from any higher node during an election
        self.got_ok = False
        self.last_election_start_ts = None
        self.leader_election_ts = None  # When current leader was elected (for grace period)
        self.current_election_id = None  # Unique ID for current election to prevent timeout race conditions
        
        # Track last membership update from leader (for followers)
        self.last_membership_update = time.time()
        
        # Reliable Total Order Multicast (ATOM) - all nodes
        self.next_seq_to_deliver = 1  # Next sequence number to deliver
        self.holdback = {}  # seq -> message (out-of-order messages waiting to be delivered)
        
        # ATOM - leader only
        self.next_seq_to_assign = 1  # Next sequence number to assign
        self.pending_acks = {}  # (seq, msg_id) -> set of peer_ids missing ACK
        self.ack_retry_count = {}  # (seq, msg_id) -> retry count
        
        # Reliable PROPOSE sending - track messages until they're ordered
        # Handles case where leader crashes before ordering a PROPOSE
        self.pending_proposes = {}  # mid -> {"payload": {}, "tries": int, "last_try": time, "sender_id": int}
        self.MAX_PROPOSE_RETRIES = 10
        self.PROPOSE_RETRY_TIMEOUT = 2.0  # Retry every 2 seconds
        
        # Create sockets
        # Use port 0 to let OS assign a free unicast port (prevents conflicts on same machine)
        self.unicast_sock = make_unicast_socket("0.0.0.0", 0)
        self.multicast_sock = make_multicast_listener_socket(MCAST_GRP, MCAST_PORT)
        self.multicast_sender_sock = make_multicast_sender_socket()  # Separate socket for sending multicast
        
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
        
        # Reliable PROPOSE retransmit thread (followers retry their PROPOSEs)
        reliable_propose_thread = threading.Thread(
            target=self._reliable_propose_retransmit_loop,
            daemon=True,
            name=f"Node-{self.node_id}-ReliablePropose"
        )
        reliable_propose_thread.start()
    
    def _system_log(self, msg):
        """
        Log a system/control-plane message (not visible to chat UI).
        Use this for DISCOVERY, HEARTBEAT, ELECTION, MEMBERSHIP, COORDINATOR, etc.
        These are internal protocol messages, not user chat.
        """
        # System messages are logged but NOT printed to stdout/chat
        # Uncomment below for debugging:
        #print(f"[SYSTEM] {msg}")
        pass
    
    def start(self):
        """Start node: begin listeners, then perform discovery."""
        # Start UI thread first
        self.ui.start()
        
        self._system_log("Starting listeners...")
        time.sleep(0.1)  # Brief delay to ensure threads start
        
        self._system_log("Beginning discovery...")
        self.startup_discovery()
        
        if self.is_leader:
            self._system_log(f"Elected as LEADER (term={self.term})")
        else:
            self._system_log(f"Joined cluster under leader {self.leader_id} at {self.leader_addr}")
            # Give new node time to settle and receive initial messages
            time.sleep(0.5)
    
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
            self._system_log(f"Sent DISCOVERY (attempt {attempt + 1}/{DISCOVERY_RETRIES})")
            
            # Wait for reply from listener thread
            time.sleep(DISCOVERY_TIMEOUT_SEC)
            
            if self.discovery_reply:
                # Got a reply from listener thread
                reply, addr = self.discovery_reply
                payload = reply.get("payload", {})
                self.leader_id = payload.get("leader_id")
                self.leader_addr = addr
                self.term = payload.get("term", 0)
                
                # Normalize all addresses in members dict from list to tuple (JSON converts tuples to lists)
                raw_members = payload.get("members", {})
                self.members = {}
                for member_id, member_addr in raw_members.items():
                    if isinstance(member_addr, list):
                        self.members[member_id] = tuple(member_addr)
                    else:
                        self.members[member_id] = member_addr
                
                # Initialize last_seen to NOW for all members
                now = time.time()
                self.last_seen = {member_id: now for member_id in self.members.keys()}
                
                # Initialize vector clock with all members from cluster
                self.vc = VectorClock(self.node_id, [int(mid) for mid in self.members.keys()])
                
                self._system_log(f"Received DISCOVERY_REPLY from leader {self.leader_id}")
                mcast_send_sock.close()
                return
        
        # No reply received - self-elect as leader
        self.is_leader = True
        self.leader_id = self.node_id
        self.leader_addr = self.unicast_sock.getsockname()
        self.term += 1
        # Use string keys for members to keep JSON and runtime consistent
        self.members[str(self.node_id)] = self.leader_addr
        self.last_seen[str(self.node_id)] = time.time()
        # Initialize vector clock with just this node
        self.vc = VectorClock(self.node_id, [self.node_id])
        mcast_send_sock.close()
        self._system_log("No leader found, self-electing")
    
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
                    # Include the max sequence number this follower has delivered
                    # This helps new leader know where to continue sequence numbering
                    max_seq_delivered = self.next_seq_to_deliver - 1  # -1 because next_seq_to_deliver is the NEXT one expected
                    payload = {"max_seq_delivered": max_seq_delivered}
                    msg = make_msg(HEARTBEAT, self.node_id, self.term, payload)
                    send_json(self.unicast_sock, self.leader_addr, msg)
                    # Uncomment for debugging:
                    # print(f"Node {self.node_id}: Sent HEARTBEAT to leader at {self.leader_addr} (max_seq={max_seq_delivered})")
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
                
                # Suppress failure detection during elections to prevent overlap
                if self.election_in_progress:
                    continue
                
                # Grace period: don't remove nodes for FAILURE_TIMEOUT_SEC after becoming leader
                # This gives followers time to send their first heartbeat
                now = time.time()
                if self.leader_election_ts and (now - self.leader_election_ts) < FAILURE_TIMEOUT_SEC:
                    continue  # Skip failure detection during grace period
                
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
                    self._system_log(f"Detected failure of node {node_id} (no heartbeat for {FAILURE_TIMEOUT_SEC}s)")
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
            "last_seen": self.last_seen,
            "next_seq": self.next_seq_to_assign  # Tell followers what sequence to expect next
        }
        msg = make_msg(MEMBERSHIP, self.node_id, self.term, payload)
        
        # Send to all peers (skip those that fail)
        for member_id, addr in list(self.members.items()):
            try:
                if int(member_id) == self.node_id:  # Don't send to self
                    continue
            except Exception:
                # If member_id is not numeric for some reason, fall back to string compare
                if member_id == str(self.node_id):
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
        self.got_ok = False  # Reset OK flag for this election
        
        # Generate unique election ID to prevent timeout race conditions
        # If a COORDINATOR arrives before timeout, this election_id will be stale
        import uuid
        self.current_election_id = str(uuid.uuid4())
        election_id = self.current_election_id
        
        # NOTE: Do NOT increment term here. In Bully algorithm, leadership is determined
        # solely by node_id, not by term/epoch. Only COORDINATOR messages carry new terms.
        self.last_election_start_ts = time.time()
        
        print(f"Node {self.node_id}: Starting election (term={self.term})")
        self._system_log(f"Starting election (term={self.term})")
        
        # Get IDs of peers with higher node_id
        higher = self.higher_ids()
        
        if not higher:
            # No higher IDs - self becomes leader
            self._become_leader()
        else:
            # Send ELECTION to all higher IDs
            msg = make_msg(ELECTION, self.node_id, self.term)
            for peer_id in higher:
                peer_key = str(peer_id)
                if peer_key in self.members:
                    peer_addr = self.members[peer_key]
                    try:
                        send_json(self.unicast_sock, peer_addr, msg)
                        self._system_log(f"Sent ELECTION to higher peer {peer_id}")
                    except Exception as e:
                        self._system_log(f"Failed to send ELECTION to {peer_id}: {e}")
            
            # Wait for OK response
            self.awaiting_coordinator = True
            
            # Start timer for election timeout
            def election_timeout():
                time.sleep(ELECTION_TIMEOUT_SEC)
                
                # Only proceed if this is still the current election
                # (COORDINATOR may have arrived and started a new election)
                if self.current_election_id != election_id:
                    return  # Stale election timeout, ignore
                
                # Only become leader if we did NOT receive any OK response
                # (got_ok is set to True when ELECTION_OK arrives)
                if not self.got_ok:
                    # No OK received - become leader
                    print(f"Node {self.node_id}: No OK received within {ELECTION_TIMEOUT_SEC}s, becoming leader")
                    self._system_log(f"No OK received within {ELECTION_TIMEOUT_SEC}s, becoming leader")
                    self._become_leader()
            
            timeout_thread = threading.Thread(target=election_timeout, daemon=True)
            timeout_thread.start()
    
    def _check_leader_alive(self):
        """
        Followers check if leader is still alive by tracking last_seen[leader_id].
        
        Uses last_seen timestamp (updated on ANY message from leader: HEARTBEAT, MEMBERSHIP, etc.)
        NOT MEMBERSHIP updates alone, to avoid false timeouts during elections or delays.
        """
        while True:
            try:
                time.sleep(1.0)
                
                # Only followers check leader (and suppress during own elections)
                if self.is_leader or not self.leader_id or self.election_in_progress:
                    continue
                
                # Check if we have seen the leader recently using last_seen tracking
                # last_seen is updated on ANY message from a peer (heartbeat, membership, etc.)
                now = time.time()
                leader_key = str(self.leader_id)
                last_seen_time = self.last_seen.get(leader_key, now)
                
                if now - last_seen_time > FAILURE_TIMEOUT_SEC:
                    print(f"Node {self.node_id}: Leader {self.leader_id} appears to be dead (no message for {FAILURE_TIMEOUT_SEC}s)")
                    self._system_log(f"Leader {self.leader_id} appears dead (no message for {FAILURE_TIMEOUT_SEC}s)")
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
                # Read line from stdin (input() shows its own prompt)
                text = input(f"Node {self.node_id}> ")
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
                    if not self.leader_addr:
                        self._system_log(f"Error: Don't know leader address yet, cannot send message. Wait for discovery.")
                        print(f"Node {self.node_id}: DEBUG - leader_id={self.leader_id}, leader_addr={self.leader_addr}, members={self.members}")
                        continue
                    
                    propose_msg = make_msg(PROPOSE, self.node_id, self.term, payload)
                    try:
                        self._system_log(f"Sending PROPOSE to leader {self.leader_id} at {self.leader_addr}")
                        print(f"Node {self.node_id}: DEBUG - Attempting to send PROPOSE to {self.leader_addr}, leader_id={self.leader_id}, is_leader={self.is_leader}, term={self.term}")
                        send_json(self.unicast_sock, self.leader_addr, propose_msg)
                        self._system_log(f"Proposed message '{text}'")
                        print(f"Node {self.node_id}: Message sent successfully")
                        
                        # Track this PROPOSE for reliable delivery
                        self.pending_proposes[mid] = {
                            "payload": payload,
                            "tries": 0,
                            "last_try": time.time(),
                            "sender_id": self.node_id
                        }
                        self._system_log(f"Tracking PROPOSE {mid} for reliable delivery")
                    except Exception as e:
                        self._system_log(f"Failed to send PROPOSE to leader: {e}")
                        print(f"Node {self.node_id}: DEBUG - Failed to send PROPOSE: {e}")
            
            except Exception as e:
                # Ignore input errors, continue loop
                pass
    
    def _order_message(self, msg_id, payload, sender_id=None):
        """
        Leader orders a message with a sequence number.
        
        Args:
            msg_id: Unique message identifier
            payload: Message payload dict with "text" and other data
            sender_id: Original sender's node ID (if None, defaults to leader's ID)
        """
        # If sender_id not provided, use leader's ID (for messages sent directly by leader)
        if sender_id is None:
            sender_id = self.node_id
        
        # Increment vector clock when ordering a message
        self.vc.increment()
        
        # Assign sequence number
        seq = self.next_seq_to_assign
        self.next_seq_to_assign += 1
        
        # Get current vector clock as snapshot
        vc_snapshot = self.vc.get_clock()
        
        # Create ORDERED message with vector clock and original sender ID
        ordered_payload = {
            "seq": seq,
            "mid": msg_id,
            "payload": payload,
            "vc": vc_snapshot,  # Include vector clock for causal ordering
            "original_sender_id": sender_id  # Preserve original sender ID
        }
        ordered_msg = make_msg(ORDERED, self.node_id, self.term, ordered_payload)
        
        # Send to all group members via multicast (includes all followers)
        # This is more efficient than unicast to each member individually
        # Use dedicated multicast sender socket (not unicast socket)
        try:
            send_json(self.multicast_sender_sock, (MCAST_GRP, MCAST_PORT), ordered_msg)
            self._system_log(f"Multicast ORDERED message seq={seq} to group")
        except Exception as e:
            self._system_log(f"Failed to multicast ORDERED message: {e}")
        
        # Leader delivers its own message locally (don't send to self via network)
        # This avoids UDP loopback issues and ensures synchronous delivery
        chat_text = payload.get("text", "")
        # Use the original sender_id for the ChatMessage
        chat_msg = ChatMessage(sender_id=sender_id, text=chat_text, sequence_number=seq, vector_clock=vc_snapshot)
        
        # Store in holdback queue and deliver if it's next in sequence
        self.holdback[seq] = chat_msg
        while self.next_seq_to_deliver in self.holdback:
            deliver_msg = self.holdback.pop(self.next_seq_to_deliver)
            # Display message via UI (thread-safe, non-blocking)
            self.ui.display_message(deliver_msg.sender_id, deliver_msg.text, deliver_msg.sequence_number)
            self.next_seq_to_deliver += 1
        
        # Initialize pending ACKs for this (seq, msg_id)
        # Track which members need to ACK (all except leader itself)
        pending_set = set()
        for member_id in self.members.keys():
            member_id_int = int(member_id) if isinstance(member_id, str) else member_id
            if member_id_int != self.node_id:
                pending_set.add(member_id)
        
        self.pending_acks[(seq, msg_id)] = pending_set
        self._system_log(f"Ordered message seq={seq}, mid={msg_id}, from node {sender_id}, waiting for ACKs from {len(pending_set)} peers")
    
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
                    self._system_log(f"Max retries exceeded for (seq={seq}, mid={msg_id}). Removing {len(missing_peers)} peers from cluster")
                    
                    for peer_id in missing_peers:
                        if peer_id in self.members:
                            del self.members[peer_id]
                    
                    # Broadcast updated membership
                    self._broadcast_membership()
                    
                    # Clean up retry count
                    del self.ack_retry_count[(seq, msg_id)]
                    continue
                
                # Resend ORDERED to peers still missing ACK
                self._system_log(f"Retransmitting (seq={seq}, mid={msg_id}) to {len(missing_peers)} peers (retry #{self.ack_retry_count[(seq, msg_id)]})")
                
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
    
    def _reliable_propose_retransmit_loop(self):
        """
        Followers retransmit PROPOSE messages until they're ordered.
        
        Handles the case where:
        1. Follower sends PROPOSE to leader
        2. Leader crashes before ordering it
        3. New leader takes over
        4. Follower keeps retrying until new leader orders it
        
        Once the PROPOSE is ordered (received as ORDERED message),
        we remove it from pending_proposes.
        """
        while True:
            try:
                time.sleep(self.PROPOSE_RETRY_TIMEOUT)
                
                # Only followers do this (leaders don't send PROPOSEs)
                if self.is_leader:
                    continue
                
                # Retry each pending PROPOSE
                current_time = time.time()
                for msg_id, propose_info in list(self.pending_proposes.items()):
                    # Check if time for retry
                    if current_time - propose_info.get("last_try", 0) < self.PROPOSE_RETRY_TIMEOUT:
                        continue
                    
                    # Check retry limit
                    tries = propose_info.get("tries", 0)
                    if tries >= self.MAX_PROPOSE_RETRIES:
                        self._system_log(f"Max retries exceeded for PROPOSE {msg_id}, giving up")
                        del self.pending_proposes[msg_id]
                        continue
                    
                    # Increment try count
                    self.pending_proposes[msg_id]["tries"] = tries + 1
                    self.pending_proposes[msg_id]["last_try"] = current_time
                    
                    # Resend PROPOSE to leader
                    if self.leader_addr:
                        payload = propose_info.get("payload", {})
                        propose_msg = make_msg(PROPOSE, self.node_id, self.term, payload)
                        try:
                            send_json(self.unicast_sock, self.leader_addr, propose_msg)
                            self._system_log(f"Retransmitting PROPOSE {msg_id} (try #{tries + 1}/{self.MAX_PROPOSE_RETRIES})")
                        except Exception as e:
                            self._system_log(f"Failed to retransmit PROPOSE {msg_id}: {e}")
            except Exception as e:
                print(f"Node {self.node_id} reliable propose error: {e}")
    
    def _become_leader(self):
        """Declare self as leader and broadcast COORDINATOR."""
        self.is_leader = True
        self.leader_id = self.node_id
        self.leader_addr = self.unicast_sock.getsockname()
        self.election_in_progress = False
        self.awaiting_coordinator = False
        self.leader_election_ts = time.time()  # Mark when we became leader
        
        print(f"\n[NEW LEADER] Node {self.node_id} is leader, unicast socket at {self.leader_addr}\n")
        self._system_log(f"Elected as NEW LEADER (term={self.term}), listening at {self.leader_addr}")
        
        # Initialize members dict if empty
        if not self.members:
            # store as string key for consistency with JSON-encoded membership
            self.members[str(self.node_id)] = self.leader_addr
        
        # Reset last_seen for all members to NOW (they haven't sent heartbeats yet in new term)
        # This gives followers time to respond to COORDINATOR before we mark them as dead
        now = time.time()
        self.last_seen = {member_id: now for member_id in self.members.keys()}
        
        # CRITICAL: When becoming leader via election, sync next_seq_to_assign with previous leader's progress
        # The new leader must start from the highest sequence that has already been delivered
        # Initialize max_seq_seen_from_followers to current next_seq_to_deliver - 1
        # (what we've delivered so far tells us the highest seq that was ordered)
        self.max_seq_seen_from_followers = max(0, self.next_seq_to_deliver - 1)
        if self.next_seq_to_assign <= self.max_seq_seen_from_followers:
            old_next_seq = self.next_seq_to_assign
            self.next_seq_to_assign = self.max_seq_seen_from_followers + 1
            self._system_log(f"New leader: Synced sequence from delivered state. Old next_seq={old_next_seq}, new next_seq={self.next_seq_to_assign}")
        
        # Clear message state from old leader(s), but keep sequence counter
        self.pending_acks = {}  # Clear pending ACKs from old leader's messages
        self.ack_retry_count = {}  # Clear retry counts
        self.message_history = []  # Clear message history
        
        # Reinitialize vector clock with current members
        self.vc = VectorClock(self.node_id, [int(mid) for mid in self.members.keys()])
        
        # Broadcast COORDINATOR to all peers first (before removing any nodes)
        msg = make_msg(COORDINATOR, self.node_id, self.term, {"leader_id": self.node_id})
        print(f"Node {self.node_id}: Broadcasting COORDINATOR to members: {self.members}")
        self._system_log(f"Broadcasting COORDINATOR to {len(self.members)} members")
        
        for member_id, addr in list(self.members.items()):
            try:
                if int(member_id) == self.node_id:
                    continue
            except Exception:
                if member_id == str(self.node_id):
                    continue
            
            try:
                # Convert addr to tuple if it's a list (from JSON)
                if isinstance(addr, list):
                    addr = tuple(addr)
                print(f"Node {self.node_id}: Sending COORDINATOR to node {member_id} at {addr}")
                send_json(self.unicast_sock, addr, msg)
                self._system_log(f"Sent COORDINATOR to node {member_id} at {addr}")
            except Exception as e:
                # Silently skip dead nodes
                print(f"Node {self.node_id}: Failed to send COORDINATOR to node {member_id}: {e}")
                self._system_log(f"Failed to send COORDINATOR to node {member_id}: {e}")
        
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
        
        # Update last_seen on ANY message from a peer (use string keys)
        if from_id is not None:
            try:
                self.last_seen[str(from_id)] = time.time()
            except Exception:
                self.last_seen[from_id] = time.time()
        
        if msg_type == DISCOVERY:
            # A node is trying to discover the cluster
            if self.is_leader:
                # Add new member to cluster; normalize member key to string
                member_key = str(from_id)
                if member_key not in self.members:
                    unicast_port = msg.get("payload", {}).get("unicast_port")
                    if unicast_port:
                        member_addr = (addr[0], unicast_port)
                        self.members[member_key] = member_addr
                        self.last_seen[member_key] = time.time()
                        
                        # Update vector clock to include new member WITHOUT losing current state
                        # Just add the new member to the existing clock, don't reinitialize
                        new_member_key = str(from_id)
                        if new_member_key not in self.vc.clock:
                            self.vc.clock[new_member_key] = 0
                
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
                    self._system_log(f"Responded to DISCOVERY from node {from_id} at {reply_addr}")
        
        elif msg_type == DISCOVERY_REPLY:
            # Store reply for startup_discovery to process
            self.discovery_reply = (msg, addr)
            self._system_log(f"Listener got DISCOVERY_REPLY from {addr}")
        
        elif msg_type == HEARTBEAT:
            # Received heartbeat from a peer
            # last_seen already updated above
            payload = msg.get("payload", {})
            
            if self.is_leader:
                # Leader receiving heartbeat from follower
                # Extract follower's last delivered sequence number for sequence sync
                follower_max_seq = payload.get("max_seq_delivered", 0)
                
                # Track the highest sequence number any follower has delivered
                # New leader uses this to know where to start assigning sequences
                if not hasattr(self, 'max_seq_seen_from_followers'):
                    self.max_seq_seen_from_followers = 0
                
                self.max_seq_seen_from_followers = max(self.max_seq_seen_from_followers, follower_max_seq)
                
                # If our next_seq_to_assign is behind what followers have seen, advance it
                if self.next_seq_to_assign <= self.max_seq_seen_from_followers:
                    self._system_log(f"Follower {from_id} reported seq={follower_max_seq}, advancing next_seq_to_assign from {self.next_seq_to_assign} to {self.max_seq_seen_from_followers + 1}")
                    self.next_seq_to_assign = self.max_seq_seen_from_followers + 1

        
        elif msg_type == MEMBERSHIP:
            # Received membership update from leader
            payload = msg.get("payload", {})
            msg_term = payload.get("term", 0)
            
            # Ignore own messages or messages from non-leaders
            if from_id == self.node_id:
                return  # Ignore self messages
            
            # Only accept if term >= current term
            if msg_term >= self.term:
                old_members = set(self.members.keys())
                new_members_raw = payload.get("members", {})
                new_members = set(new_members_raw.keys())
                
                if old_members != new_members:
                    self._system_log(f"Updated membership - new members: {new_members}")
                
                self.term = msg_term
                # Normalize all addresses in members dict from list to tuple (JSON converts tuples to lists)
                raw_members = payload.get("members", {})
                self.members = {}
                for member_id, addr in raw_members.items():
                    if isinstance(addr, list):
                        self.members[member_id] = tuple(addr)
                    else:
                        self.members[member_id] = addr
                
                # Initialize last_seen to NOW for all members (don't use stale timestamps)
                now = time.time()
                self.last_seen = {member_id: now for member_id in self.members.keys()}
                self.last_membership_update = now
                
                # CRITICAL: Don't reinitialize vector clock - just add new members!
                # Reinitializing loses all causal history.
                # Instead: add any new members with clock value 0
                for member_id in self.members.keys():
                    member_id_str = str(member_id)
                    if member_id_str not in self.vc.clock:
                        self.vc.clock[member_id_str] = 0
                
                # Sync sequence number with leader if this is new membership
                if old_members != new_members:
                    leader_next_seq = payload.get("next_seq", None)
                    if leader_next_seq is not None:
                        # Leader tells us what sequence to expect next
                        # New nodes should start from here
                        self.next_seq_to_deliver = leader_next_seq
                        self.holdback = {}  # Clear any out-of-order messages from before sync
                        self._system_log(f"Synced sequence number with leader: next_seq_to_deliver={self.next_seq_to_deliver}")
                
                self._system_log(f"Updated membership from leader (term={msg_term}): members={list(self.members.keys())}")
        
        elif msg_type == ELECTION:
            # Received election from lower ID peer
            sender_id = from_id
            if sender_id < self.node_id:
                # Reply with OK
                ok_msg = make_msg(ELECTION_OK, self.node_id, self.term)
                try:
                    send_json(self.unicast_sock, addr, ok_msg)
                    self._system_log(f"Received ELECTION from node {sender_id}, sent OK")
                except Exception as e:
                    self._system_log(f"Failed to send OK to {sender_id}: {e}")
                
                # Start own election if not already in progress
                if not self.election_in_progress:
                    self.start_election()
        
        elif msg_type == ELECTION_OK:
            # Received OK from higher ID peer
            # Mark that we received an OK so election timeout won't make us leader
            self.got_ok = True
            self.awaiting_coordinator = True
            self._system_log(f"Received OK from node {from_id}, awaiting coordinator")
            
            # Reset the election timeout when we receive an OK
            # (the higher node is running, so it will elect)
            if hasattr(self, '_election_start_time'):
                self._election_start_time = time.time()
        
        elif msg_type == COORDINATOR:
            # Received coordinator announcement
            payload = msg.get("payload", {})
            coordinator_id = payload.get("leader_id", from_id)
            coordinator_term = msg.get("term", 0)
            
            print(f"Node {self.node_id}: Received COORDINATOR from {from_id} at {addr}, coordinator_id={coordinator_id}, term={coordinator_term}")

            # Normalize coordinator_id to integer for comparison
            try:
                coord_id_int = int(coordinator_id) if isinstance(coordinator_id, str) else coordinator_id
            except Exception:
                coord_id_int = coordinator_id

            # === Bully Invariant Enforcement ===
            # In the Bully algorithm, leadership is determined solely by node ID.
            # Higher-ID nodes must NEVER accept a leader with a lower ID.
            # If a lower-ID node claims leadership, reject it and initiate an election
            # to ensure the highest available node becomes leader.
            if isinstance(coord_id_int, int) and coord_id_int < self.node_id:
                # Reject coordinator from lower-ID node; start election to assert higher-id leadership
                print(f"Node {self.node_id}: Rejecting COORDINATOR from lower-id {coordinator_id}, starting election")
                self._system_log(f"Ignoring COORDINATOR from lower-id {coordinator_id}; starting election")
                if not self.election_in_progress:
                    self.start_election()
                return

            # Update if newer term or valid coordinator
            if coordinator_term >= self.term:
                self.term = coordinator_term
                self.leader_id = coordinator_id
                # Normalize leader flag
                self.is_leader = (coord_id_int == self.node_id)

                # Ensure new leader is in members dict (important if joining nodes or recovering)
                leader_key = str(coordinator_id)
                if leader_key not in self.members:
                    self.members[leader_key] = addr
                    print(f"Node {self.node_id}: Added new leader {coordinator_id} to members dict with address {addr}")
                    self._system_log(f"Added new leader {coordinator_id} to members dict with address {addr}")

                # Update leader address from members dict or from COORDINATOR sender
                if str(coordinator_id) in self.members:
                    self.leader_addr = self.members[str(coordinator_id)]
                    print(f"Node {self.node_id}: Updated leader_addr to {self.leader_addr} (from members dict)")
                    self._system_log(f"Updated leader_addr from members dict: {self.leader_addr}")
                else:
                    self.leader_addr = addr
                    print(f"Node {self.node_id}: Updated leader_addr to {addr} (from COORDINATOR sender)")
                    self._system_log(f"Updated leader_addr from COORDINATOR sender: {self.leader_addr}")
                
                print(f"Node {self.node_id}: NEW LEADER IS {coordinator_id} at {self.leader_addr}")
                
                self.election_in_progress = False
                self.awaiting_coordinator = False
                self.got_ok = False  # Reset OK flag when new coordinator is established
                self.current_election_id = None  # Clear election ID to prevent timeout race conditions
                
                # Reset leader election timestamp if we become leader
                if self.is_leader:
                    self.leader_election_ts = time.time()
                # Followers do NOT reset sequence numbers - they continue expecting the next seq
                # (sequence numbers are globally monotonic across leader transitions)
                
                # Reset last membership update timer (leader just announced itself)
                self.last_membership_update = time.time()
                
                # Print visible message for users when new leader is elected
                print(f"\n[ELECTION RESULT] Node {coordinator_id} elected as NEW LEADER (term={coordinator_term})\n")
                self._system_log(f"Received COORDINATOR - new leader is {coordinator_id} (term={coordinator_term})")
        
        elif msg_type == PROPOSE:
            # Received message proposal from peer (leader only)
            if self.is_leader:
                payload = msg.get("payload", {})
                msg_id = payload.get("mid")
                text = payload.get("text", "")
                # Pass the original sender's ID (from_id) to preserve it in ordered messages
                self._system_log(f"Leader received PROPOSE from node {from_id}: '{text}'")
                self._order_message(msg_id, payload, sender_id=from_id)
            else:
                # Non-leader ignores PROPOSE
                self._system_log(f"Ignoring PROPOSE (not leader): from node {from_id}")
                pass
        
        elif msg_type == ORDERED:
            # Received ordered message from leader
            payload = msg.get("payload", {})
            seq = payload.get("seq")
            msg_id = payload.get("mid")
            msg_payload = payload.get("payload", {})
            received_vc = payload.get("vc", {})  # Extract vector clock from message
            
            # Get the original sender ID (preserved by leader)
            original_sender_id = payload.get("original_sender_id", from_id)
            
            print(f"Node {self.node_id}: DEBUG - Received ORDERED seq={seq}, mid={msg_id}, from={from_id}, text='{msg_payload.get('text', '')}'")
            
            # Update this node's vector clock based on received message
            # If we don't have a senders node in our clock yet, add it (handles new nodes joining)
            if received_vc:
                for node_id_str, time_val in received_vc.items():
                    if node_id_str not in self.vc.clock:
                        self.vc.clock[node_id_str] = 0
                self.vc.update(received_vc)
            
            # Extract chat text from the nested payload
            chat_text = msg_payload.get("text", "")
            if not chat_text:
                # Debug: log what we received
                self._system_log(f"DEBUG: msg_payload={msg_payload}, full_payload={payload}")
            
            # Get current vector clock as snapshot for this message
            vc_snapshot = self.vc.get_clock()
            
            # Create immutable ChatMessage object with ORIGINAL sender_id and vector clock
            chat_msg = ChatMessage(sender_id=original_sender_id, text=chat_text, sequence_number=seq, vector_clock=vc_snapshot)
            
            # Store in holdback queue
            self.holdback[seq] = chat_msg
            print(f"Node {self.node_id}: STORED in holdback [seq={seq}] {original_sender_id}: {chat_text} (next_seq_to_deliver={self.next_seq_to_deliver})")
            
            # Clean up reliable delivery tracking if this was our PROPOSE
            if msg_id in self.pending_proposes:
                self._system_log(f"PROPOSE {msg_id} was ordered, stopping retransmission")
                del self.pending_proposes[msg_id]
            
            # Send ACK back to leader
            ack_msg = make_msg(DELIVER_ACK, self.node_id, self.term, {"seq": seq, "mid": msg_id})
            try:
                send_json(self.unicast_sock, self.leader_addr, ack_msg)
            except Exception as e:
                self._system_log(f"Failed to send DELIVER_ACK to leader: {e}")
            
            # Deliver all messages with seq == next_seq_to_deliver in order
            while self.next_seq_to_deliver in self.holdback:
                chat_msg = self.holdback.pop(self.next_seq_to_deliver)
                # Display message via UI (thread-safe, non-blocking)
                print(f"Node {self.node_id}: DELIVERING [seq={chat_msg.sequence_number}] {chat_msg.sender_id}: {chat_msg.text}")
                self.ui.display_message(chat_msg.sender_id, chat_msg.text, chat_msg.sequence_number)
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
                        self._system_log(f"All peers ACKed (seq={seq}, mid={msg_id})")
