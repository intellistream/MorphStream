import socket
import threading
import time

# Define the target IP and port
target_ip = '127.0.0.1'  # Replace with your target IP address
target_port = 9090       # Replace with your target port number

# Customize your packet content here
deposit_content = b'deposit\n'
transfer_content = b'transfer\n'

# Customize the number of connections and packets
num_connections = 50
num_packets_per_connection = 100

def send_packet(sock,num_packets):
    for _ in range(num_packets):
        sock.send(deposit_content)
        time.sleep(0.1)
    sock.close()

# Create and establish multiple connections
for _ in range(num_connections):
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((target_ip, target_port))
        print(f"Connected to {target_ip}:{target_port}")
        
        # Create a thread for each connection to send packets
        thread = threading.Thread(target=send_packet, args=(client_socket, num_packets_per_connection))
        thread.start()
    except Exception as e:
        print(f"Connection to {target_ip}:{target_port} failed: {str(e)}")

# Wait for all threads to finish
for thread in threading.enumerate():
    if thread != threading.current_thread():
        thread.join()

print("All connections closed.")