"""
Terminal-based chat UI using curses for better display.
Displays messages and debug logs with toggle button.
"""
import threading
import curses
from queue import Queue, Empty
from datetime import datetime
import time
import os


class ChatUI:
    """Curses-based chat UI with message and debug log display."""
    
    def __init__(self, node_id):
        """
        Initialize chat UI.
        
        Args:
            node_id: This node's ID
        """
        self.node_id = node_id
        self.message_queue = Queue()  # Thread-safe queue for messages to display
        self.debug_queue = Queue()    # Thread-safe queue for debug logs
        self.input_queue = Queue()    # Thread-safe queue for user input
        self.lock = threading.Lock()
        self.ui_thread = None
        self.running = False
        self.show_debug = False
        self.messages = []
        self.debug_logs = []
        self.stdscr = None
        
        # Set up log file
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.log_file = os.path.join(log_dir, f"node_{node_id}.log")
        self._write_log(f"=== Node {node_id} Session Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
    
    def display_message(self, sender_id, text, seq):
        """
        Display a delivered chat message (thread-safe).
        
        Args:
            sender_id: ID of message sender
            text: Chat message text
            seq: Sequence number from total-order multicast
        """
        timestamp = datetime.now().strftime("%H:%M:%S")
        marker = "YOU" if sender_id == self.node_id else f"Node {sender_id}"
        formatted_msg = f"[{timestamp}] [seq={seq}] {marker}: {text}"
        self.message_queue.put(formatted_msg)
        self._write_log(f"MSG: {formatted_msg}")
    
    def display_debug(self, msg):
        """
        Display a debug/system message (thread-safe).
        
        Args:
            msg: Debug message text
        """
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_msg = f"[{timestamp}] {msg}"
        self.debug_queue.put(formatted_msg)
        self._write_log(f"DEBUG: {formatted_msg}")
    
    def _write_log(self, msg):
        """Write message to log file (thread-safe)."""
        try:
            with self.lock:
                with open(self.log_file, 'a') as f:
                    f.write(msg + '\n')
        except Exception as e:
            pass
    
    def save_logs(self):
        """Save all messages and debug logs to file."""
        try:
            with self.lock:
                with open(self.log_file, 'a') as f:
                    f.write("\n=== MESSAGES ===\n")
                    for msg in self.messages:
                        f.write(msg + '\n')
                    f.write("\n=== DEBUG LOGS ===\n")
                    for debug in self.debug_logs:
                        f.write(debug + '\n')
                    f.write(f"\n=== Session Ended at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        except Exception as e:
            pass
    
    def start(self):
        """Start the UI thread."""
        self.running = True
        self.ui_thread = threading.Thread(target=self._ui_loop, daemon=True)
        self.ui_thread.start()
    
    def _ui_loop(self):
        """Main UI event loop."""
        try:
            self.stdscr = curses.initscr()
            curses.noecho()
            curses.cbreak()
            self.stdscr.nodelay(True)  # Non-blocking getch()
            
            # Colors
            curses.start_color()
            curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)  # Messages (pink/magenta)
            curses.init_pair(2, curses.COLOR_YELLOW, curses.COLOR_BLACK)  # Debug
            curses.init_pair(3, curses.COLOR_WHITE, curses.COLOR_BLUE)    # Title
            
            self._run_ui()
            
        finally:
            if self.stdscr:
                curses.echo()
                curses.nocbreak()
                curses.endwin()
    
    def _run_ui(self):
        """Run the UI display loop."""
        input_buffer = ""
        msg_scroll = 0
        debug_scroll = 0
        
        while self.running:
            try:
                max_y, max_x = self.stdscr.getmaxyx()
                self.stdscr.clear()
                
                # Title
                title = f"Node {self.node_id} - Chat System"
                self.stdscr.addstr(0, 0, title, curses.color_pair(3) | curses.A_BOLD)
                self.stdscr.addstr(1, 0, "=" * min(max_x, len(title) + 20))
                
                # Instructions
                instructions = "Press 'Ctrl+L' to toggle debug logs | 'UP/DOWN' to scroll | 'Ctrl+Q' to quit"
                self.stdscr.addstr(2, 0, instructions[:max_x-1])
                
                # Process messages from queue
                try:
                    while True:
                        msg = self.message_queue.get_nowait()
                        self.messages.append(msg)
                except Empty:
                    pass
                
                # Process debug logs from queue
                try:
                    while True:
                        debug_msg = self.debug_queue.get_nowait()
                        self.debug_logs.append(debug_msg)
                except Empty:
                    pass
                
                # Calculate layout
                header_lines = 3
                if self.show_debug:
                    msg_height = (max_y - header_lines - 5) // 2
                    debug_start_row = header_lines + msg_height + 2
                    debug_height = max_y - debug_start_row - 2
                else:
                    msg_height = max_y - header_lines - 3
                    debug_start_row = None
                    debug_height = 0
                
                # Display messages section
                msg_label_row = header_lines
                self.stdscr.addstr(msg_label_row, 0, "MESSAGES:", curses.color_pair(1) | curses.A_BOLD)
                
                msg_content_start = msg_label_row + 1
                if self.messages:
                    # Adjust scroll position
                    total_msgs = len(self.messages)
                    msg_scroll = max(0, min(msg_scroll, total_msgs - msg_height))
                    
                    # Display scrolled messages
                    for i in range(msg_height):
                        msg_idx = msg_scroll + i
                        if msg_idx < len(self.messages):
                            try:
                                display_msg = self.messages[msg_idx]
                                if len(display_msg) > max_x - 2:
                                    display_msg = display_msg[:max_x-2]
                                self.stdscr.addstr(msg_content_start + i, 0, display_msg, curses.color_pair(1))
                            except curses.error:
                                pass
                    
                    # Show scroll indicator
                    if total_msgs > msg_height:
                        scroll_indicator = f"[{msg_scroll + 1}-{min(msg_scroll + msg_height, total_msgs)}/{total_msgs}]"
                        try:
                            self.stdscr.addstr(msg_content_start, max_x - len(scroll_indicator) - 1, scroll_indicator, curses.color_pair(1))
                        except curses.error:
                            pass
                
                # Display debug logs if toggled
                if self.show_debug and debug_start_row:
                    self.stdscr.addstr(debug_start_row, 0, "DEBUG LOGS:", curses.color_pair(2) | curses.A_BOLD)
                    
                    debug_content_start = debug_start_row + 1
                    if self.debug_logs:
                        # Get latest logs in reverse order
                        total_debug = len(self.debug_logs)
                        reversed_logs = list(reversed(self.debug_logs))
                        
                        # Adjust debug scroll position
                        debug_scroll = max(0, min(debug_scroll, total_debug - debug_height))
                        
                        # Display scrolled debug logs (latest first)
                        for i in range(debug_height):
                            log_idx = debug_scroll + i
                            if log_idx < len(reversed_logs):
                                try:
                                    display_log = reversed_logs[log_idx]
                                    if len(display_log) > max_x - 2:
                                        display_log = display_log[:max_x-2]
                                    self.stdscr.addstr(debug_content_start + i, 0, display_log, curses.color_pair(2))
                                except curses.error:
                                    pass
                        
                        # Show scroll indicator
                        if total_debug > debug_height:
                            scroll_indicator = f"[{debug_scroll + 1}-{min(debug_scroll + debug_height, total_debug)}/{total_debug}]"
                            try:
                                self.stdscr.addstr(debug_content_start, max_x - len(scroll_indicator) - 1, scroll_indicator, curses.color_pair(2))
                            except curses.error:
                                pass
                
                # Input prompt at bottom
                input_line = max_y - 1
                prompt = f"Node {self.node_id}> "
                try:
                    self.stdscr.addstr(input_line, 0, prompt, curses.A_BOLD)
                    self.stdscr.addstr(input_line, len(prompt), input_buffer[:max_x - len(prompt) - 2])
                except curses.error:
                    pass
                
                self.stdscr.refresh()
                
                # Handle input
                try:
                    self.stdscr.timeout(50)  # 50ms timeout
                    ch = self.stdscr.getch()
                    if ch != -1:  # -1 means timeout/no input
                        if ch == ord('\n') or ch == 10 or ch == 13:  # Enter key
                            if input_buffer.strip():
                                self.input_queue.put(input_buffer.strip())
                                input_buffer = ""
                        elif ch == 12:  # Ctrl+L for debug logs toggle
                            self.show_debug = not self.show_debug
                            debug_scroll = 0
                        elif ch == 17:  # Ctrl+Q to quit
                            self.running = False
                        elif ch == 127 or ch == curses.KEY_BACKSPACE or ch == 8:  # Backspace
                            input_buffer = input_buffer[:-1]
                        elif ch == curses.KEY_UP:  # Scroll up
                            if self.show_debug:
                                debug_scroll = max(0, debug_scroll - 1)
                            else:
                                msg_scroll = max(0, msg_scroll - 1)
                        elif ch == curses.KEY_DOWN:  # Scroll down
                            if self.show_debug:
                                debug_scroll = min(len(self.debug_logs) - 1, debug_scroll + 1)
                            else:
                                msg_scroll = min(len(self.messages) - 1, msg_scroll + 1)
                        elif 32 <= ch <= 126:  # Printable ASCII characters
                            input_buffer += chr(ch)
                except:
                    pass
                
                time.sleep(0.02)
                
            except Exception:
                pass
