import socket

# Your Kafka broker URLs and port
brokers = [
    ("b-1.sitmskcluster.cymah8.c4.kafka.ap-south-1.amazonaws.com", 9096),
    ("b-2.sitmskcluster.cymah8.c4.kafka.ap-south-1.amazonaws.com", 9096)
]

timeout_seconds = 5

print("Starting network connectivity test...\n")

for host, port in brokers:
    try:
        print(f"Testing connection to {host}:{port} ...")
        # Attempt to open a TCP socket
        sock = socket.create_connection((host, port), timeout=timeout_seconds)
        print(f"✅ SUCCESS: Successfully connected to {host} on port {port}.")
        sock.close()
    except socket.timeout:
        print(f"❌ TIMEOUT: Connection to {host}:{port} timed out after {timeout_seconds} seconds.")
    except socket.error as e:
        print(f"❌ ERROR: Failed to connect to {host}:{port}. Error: {e}")
    except Exception as e:
         print(f"❌ UNEXPECTED ERROR: {e}")
    print("-" * 60)

print("\nTest complete.")