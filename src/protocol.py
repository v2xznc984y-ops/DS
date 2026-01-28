import time
import uuid

# Message Types
DISCOVERY = "DISCOVERY"
DISCOVERY_REPLY = "DISCOVERY_REPLY"
HEARTBEAT = "HEARTBEAT"
MEMBERSHIP = "MEMBERSHIP"
ELECTION = "ELECTION"
ELECTION_OK = "ELECTION_OK"
COORDINATOR = "COORDINATOR"
PROPOSE = "PROPOSE"
PROPOSE_ACK = "PROPOSE_ACK"  # Leader acknowledges PROPOSE with sequence number
ORDERED = "ORDERED"
DELIVER_ACK = "DELIVER_ACK"


def now():
    """Return current time as timestamp."""
    return time.time()


def msg_id():
    """Generate unique message ID using UUID4."""
    return str(uuid.uuid4())


def make_msg(mtype, from_id, term, payload=None):
    """
    Create a message dictionary.
    
    Args:
        mtype: Message type (string)
        from_id: Sender node ID
        term: Current term/epoch
        payload: Optional message payload
        
    Returns:
        Dictionary with keys: type, from_id, term, ts, payload
    """
    return {
        "type": mtype,
        "from_id": from_id,
        "term": term,
        "ts": now(),
        "payload": payload
    }
