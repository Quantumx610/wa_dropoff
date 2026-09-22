import socket

# host = "b-1.sitmskcluster.cymah8.c4.kafka.ap-south-1.amazonaws.com"
host= "b-2.sitmskcluster.cymah8.c4.kafka.ap-south-1.amazonaws.com"
port = 9096

try:
    # Create a socket connection with a 5-second timeout
    sock = socket.create_connection((host, port), timeout=30)
    print(f"Successfully connected to {host} on port {port}!")
    sock.close()
except socket.timeout:
    print(f"Connection timed out while trying to reach {host}:{port}")
except socket.error as e:
    print(f"Connection failed: {e}")