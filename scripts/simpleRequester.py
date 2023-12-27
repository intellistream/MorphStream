import socket
import threading
import time
import argparse

# Define the target IP and port
target_ip = '127.0.0.1'  # Replace with your target IP address
target_port = 9090       # Replace with your target port number

# Customize your packet content here
deposit_content = b'deposit\n'
transfer_content = b'transfer\n'

# Customize the number of connections and packets
num_connections = 50
num_packets_per_connection = 100



def request_naive_SL():
    def send_deposit(sock, content, num_packets):
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
            thread = threading.Thread(target=send_deposit, args=(client_socket, num_packets_per_connection))
            thread.start()
        except Exception as e:
            print(f"Connection to {target_ip}:{target_port} failed: {str(e)}")

    # Wait for all threads to finish
    for thread in threading.enumerate():
        if thread != threading.current_thread():
            thread.join()

def read_and_send(tid, fileName, socket):
    try:
        with open(fileName, "r") as f:
            line_number = 1 
            for line in f:
                if line_number % num_connections == tid:
                    socket.send(line.encode('ascii'))
                    time.sleep(0.1)
                line_number += 1
        socket.close()
    except FileNotFoundError:
        print(f"File not found: {fileName}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def request_SL(fileName):
    for tid in range(num_connections):
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((target_ip, target_port))
            print(f"Connected to {target_ip}:{target_port}")
            
            # Create a thread for each connection to send packets
            thread = threading.Thread(target=read_and_send, args=(tid, fileName ,client_socket))
            thread.start()
        except Exception as e:
            print(f"Connection to {target_ip}:{target_port} failed: {str(e)}")

    # Wait for all threads to finish
    for thread in threading.enumerate():
        if thread != threading.current_thread():
            thread.join()

def main():
    parser = argparse.ArgumentParser(description="Example to report.")
    # Add an optional argument for the file name
    parser.add_argument("-F", "--fileName", help="Specify the input stream file name")

    args = parser.parse_args()
    if args.fileName:
        request_SL(args.fileName)
    else:
        request_naive_SL()

if __name__ == "__main__":
    main()
