import socket
import json


def send_json(sock, addr, msg):
    """
    Send JSON message over UDP socket.
    
    Args:
        sock: UDP socket
        addr: Tuple (host, port) to send to
        msg: Dictionary to send as JSON
    """
    data = json.dumps(msg).encode('utf-8')
    sock.sendto(data, addr)


def recv_json(sock, bufsize=65535):
    """
    Receive JSON message from UDP socket.
    
    Args:
        sock: UDP socket
        bufsize: Buffer size (default 65535 bytes)
        
    Returns:
        Tuple (data_dict, addr) where data_dict is parsed JSON and addr is sender address
    """
    data, addr = sock.recvfrom(bufsize)
    msg = json.loads(data.decode('utf-8'))
    return msg, addr


def make_unicast_socket(bind_ip="0.0.0.0", bind_port=0):
    """
    Create UDP unicast socket bound to specified IP and port.
    
    Args:
        bind_ip: IP address to bind to (default "0.0.0.0")
        bind_port: Port to bind to (default 0 for auto-assign)
        
    Returns:
        UDP socket object
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((bind_ip, bind_port))
    return sock


def make_multicast_listener_socket(group, port):
    """
    Create and configure UDP multicast listener socket.
    Joins the specified multicast group and enables SO_REUSEADDR and SO_REUSEPORT.
    Cross-platform compatible (macOS/Linux).
    
    Args:
        group: Multicast group address (e.g., "239.1.2.3")
        port: Port to listen on
        
    Returns:
        UDP socket joined to multicast group
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # Allow multiple sockets on same port (for multiple nodes on same machine)
    if hasattr(socket, 'SO_REUSEPORT'):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    
    sock.bind(('', port))
    
    # Join multicast group
    mreq = socket.inet_aton(group) + socket.inet_aton('0.0.0.0')
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    
    return sock
