"""
Simple terminal-based chat UI for distributed system.
Handles clean output without blocking network threads.
"""
import threading
import sys
from queue import Queue


class ChatUI:
    """Terminal-based chat UI with non-blocking message display."""
    
    def __init__(self, node_id):
        """
        Initialize chat UI.
        
        Args:
            node_id: This node's ID
        """
        self.node_id = node_id
        self.message_queue = Queue()  # Thread-safe queue for messages
        self.lock = threading.Lock()
        self.ui_thread = None
        self.running = False
        self._print_header()
    
    def _print_header(self):
        """Print initial UI header with node identity."""
        print("\n" + "="*60)
        print(f"  CHAT ROOM - You are Node {self.node_id}")
        print("="*60)
        print("Type messages and press Enter to send")
        print("(Messages appear after ordering by leader)")
        print("-"*60)
        sys.stdout.flush()
    
    def display_message(self, sender_id, text, seq):
        """
        Display a delivered chat message (thread-safe).
        
        Args:
            sender_id: ID of message sender
            text: Chat message text
            seq: Sequence number from total-order multicast
        """
        self.message_queue.put((sender_id, text, seq))
    
    def start(self):
        """Start the UI output thread."""
        self.running = True
        self.ui_thread = threading.Thread(target=self._ui_loop, daemon=True)
        self.ui_thread.start()
    
    def _ui_loop(self):
        """Continuously process queued messages (runs in background)."""
        while self.running:
            try:
                # Non-blocking check for queued messages
                if not self.message_queue.empty():
                    sender_id, text, seq = self.message_queue.get(timeout=0.1)
                    self._print_message(sender_id, text, seq)
                else:
                    # Small sleep to avoid busy-waiting
                    threading.Event().wait(0.1)
            except Exception:
                pass
    
    def _print_message(self, sender_id, text, seq):
        """Print a formatted chat message to terminal."""
        with self.lock:
            # Format: [seq=12] Node 34: hello everyone
            marker = "YOU" if sender_id == self.node_id else f"Node {sender_id}"
            print(f"[seq={seq}] {marker}: {text}")
            sys.stdout.flush()
