
import getpass
import socket
import platform
import uuid
import hashlib
import urllib.request
import urllib.error

def generate_system_id():
    """Create a unique, persistent system identifier."""
    username = getpass.getuser()
    hostname = socket.gethostname()
    os_info = platform.platform()
    mac_addr = hex(uuid.getnode())
    raw_id = f"{username}-{hostname}-{os_info}-{mac_addr}"
    return hashlib.sha256(raw_id.encode()).hexdigest()[:20]

def is_online():
    """Return True if internet is reachable (google.com)."""
    try:
        # Try to connect to Google's DNS (fast)
        urllib.request.urlopen("https://www.google.com", timeout=2)
        return True
    except (urllib.error.URLError, socket.timeout):
        return False