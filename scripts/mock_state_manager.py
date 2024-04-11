import socket
import threading
import queue

cmd = {
    0: "\00\00\00\00;\11\00\00\00;\01\00\00\00:\02\00\00\00;", # notify_CC_switch: Set tupleID 1 strategy to 2 (Cache).
    2: "\02\00\00\00;\05\00\00\00;\01\00\00\00;",              # getStateFromCache: Fetch tupleID 1 content.
    3: "\03\00\00\00;\11\00\00\00;\01\00\00\00:\02\00\00\00;", # updateStateToCache: Set tupleID 1 content to 2 (int).
    4: "\04\00\00\00;\01\00\00\00\00\00\00\00;", # handleTxnFinished: Report transaction 1 handling finished.
} 

class MockStateManagerServer:
    def __init__(self, host='127.0.0.1', port=9000):
        self.host = host
        self.port = port
        self.connections = {}  # Maps ID to connection (socket object)
        self.conn_id_counter = 1
        self.running = True
        self.lock = threading.Lock()
        self.cmd_queue = queue.Queue()

    def accept_connections(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen()
            print("Server started, listening for connections..." )
            while self.running:
                client_socket, addr = server_socket.accept()
                with self.lock:
                    conn_id = self.conn_id_counter
                    self.connections[conn_id] = client_socket
                    self.conn_id_counter += 1
                print(f"Accepted connection from {addr}, assigned ID: {conn_id}")
                threading.Thread(target=self.handle_connection, args=(client_socket, conn_id)).start()

    def handle_connection(self, client_socket, conn_id):
        with client_socket:
            while self.running:
                try:
                    data = client_socket.recv(1024)
                    if data:
                        print(f"[Connection {conn_id}] Received: {data.decode()}")
                    else:
                        break
                except ConnectionResetError:
                    break
            with self.lock:
                del self.connections[conn_id]
                print(f"Connection {conn_id} closed.")

    def process_commands(self):
        while self.running:
            cmd = input("Enter command: ")
            self.cmd_queue.put(cmd)
            if cmd.startswith("show"):
                self.show_connections()
            elif cmd.startswith("req"):
                _, conn_id, cmd_to_send = cmd.split(maxsplit=2)
                self.send_command(int(conn_id), int(cmd_to_send))
            elif cmd == "exit":
                self.running = False

    def show_connections(self):
        with self.lock:
            for conn_id, conn in self.connections.items():
                addr = conn.getpeername()
                print(f"ID: {conn_id}, Target: {addr}")

    def send_command(self, conn_id, cmd_idx):
        with self.lock:
            if conn_id in self.connections:
                cmd_to_snd = cmd[cmd_idx].encode()
                self.connections[conn_id].sendall(cmd_to_snd)
                print(f"Sent command to connection {conn_id}")
            else:
                print(f"No connection with ID: {conn_id}")

    def start(self):
        threading.Thread(target=self.accept_connections).start()
        self.process_commands()

if __name__ == "__main__":
    server = MockStateManagerServer()
    server.start()
