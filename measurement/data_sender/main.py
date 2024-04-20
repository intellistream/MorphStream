import argparse
import random
import csv
from collections import defaultdict

import argparse
import random
import csv
from collections import defaultdict
import threading
import os

data = {
    "Command": "send",
    # "Command": "generate",
    "Sender": {
        "connections": 1,
        "firstSent": 500,
        "window_size": 500,
        "instances": [
            # {
            #     "IP": "172.20.0.251",
            #     "port": 9000
            # },
            # {
            #     "IP": "127.0.0.1",
            #     "port": 9000
            # },
            {
                "IP": "127.0.0.1",
                "port": 9000
            }
        ],
        "report": {
            "interval_s": 1,
            "warning_latency_ms": 500,
            "location": "history"
        }
    },
    "Generator": {
        "count_millions": 1,
        "location": "send_to"
    }
}

class RecordConfig:
    def __init__(self, host_range=(1, 10000), protocol_options=["TCP", "UDP", "HTTP", "FTP", "SSH"]):
        self.host_range = host_range
        self.protocol_options = protocol_options
        self.host_records = defaultdict(int)
        self.thread_nums = data["Sender"]["connections"]
        self._cur_thread = 0
        self._record_id = 100000

    def generate_host(self):
        return random.randint(self.host_range[0], self.host_range[1])

    def generate_is_first(self, host):
        if self.host_records[host] == 0:
            self.host_records[host] += 1
            return True
        else:
            self.host_records[host] += 1
            return False

    def generate_protocol(self):
        return random.choice(self.protocol_options)

    def generate_data_length(self):
        return random.randint(1, 1000)

    def generate_is_random(self):
        return random.choice([True, False])
    
    def generate_thread_nums(self):
        self._cur_thread += 1
        self._cur_thread %= self.thread_nums
        return self._cur_thread

class DataGenerator:
    def __init__(self, record_count):
        self.record_config = RecordConfig()
        self.generated_records = []
        self.record_count = record_count

    def generate_csv_records(self, file_path):
        # file_path = ${PythonScriptDir}/filePath
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, file_path)
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for _ in range(self.record_count):
                host = self.record_config.generate_host()
                is_first = self.record_config.generate_is_first(host)
                protocol = self.record_config.generate_protocol()
                data_length = self.record_config.generate_data_length()
                is_random = self.record_config.generate_is_random()
                thread_idx = self.record_config.generate_thread_nums()
                writer.writerow([self.record_config._record_id, host, is_first, protocol, data_length, is_random, thread_idx])
                self.generated_records.append((host, is_first, protocol, data_length, is_random))
                self.record_config._record_id += 1
        return self

    def generate_statistics(self):
        host_count = len(self.record_config.host_records)
        records_per_host = defaultdict(int)
        for host, count in self.record_config.host_records.items():
            records_per_host[count] += 1
        protocol_distribution = defaultdict(int)
        for protocol in self.record_config.protocol_options:
            protocol_distribution[protocol] = sum(record[2] == protocol for record in self.generated_records)

        return host_count, records_per_host, protocol_distribution

    def save_statistics(self, file_path, host_count, records_per_host, protocol_distribution):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, file_path)
        with open(file_path, 'w') as file:
            file.write("Statistics about the generated batch:\n")
            file.write(f"Number of unique hosts: {host_count}\n")
            file.write("Records per host distribution:\n")
            for records, count in sorted(records_per_host.items()):
                file.write(f"{records} records: {count}\n")
            file.write("Protocol distribution:\n")
            for protocol, count in protocol_distribution.items():
                file.write(f"{protocol}: {count}\n")
            file.write(f"Record count: {self.record_count}\n")
            file.write(f"Connection count: {self.record_config.thread_nums}\n")

import csv
import os
import socket
import time
import signal

class Sender:
    def __init__(self, ip_address, port):
        self.ip_address = ip_address
        self.port = port
        self.connections = []
        self.records = []
        self.record_status = {}
        self.sent_cnt = 0
        self.received_cnt = 0
        self.max_connections = data["Sender"]["connections"]

    def connect_to_server(self):
        while len(self.connections) < self.max_connections:
            try:
                connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                connection.connect((self.ip_address, self.port))
                self.connections.append(connection)
                print(f"Connection {len(self.connections)}: Successfully connected to {self.ip_address}:{self.port}")
            except Exception as e:
                print(f"Connection failed {len(self.connections)}: {e}")
                time.sleep(1)
                continue

    def send_records(self, csv_file):
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            self.records = list(reader)
            self.record_status = {int(r[0]): (0, 0) for r in self.records}

        for connection in self.connections:
            cnt = self.sent_cnt
            self.sent_cnt += data["Sender"]["firstSent"]
            batch = self.records[cnt:cnt+data["Sender"]["firstSent"]]
            for r in batch:
                self.record_status[int(r[0])] = (time.time_ns(), 0)
                connection.sendall((",".join(r) + '\n').encode())
        
        while True:
            buffer = bytearray()
            for connection in self.connections:
                try:
                    rcv = connection.recv(4096)
                    if rcv:
                        buffer += rcv
                        while b'\n' in buffer:
                            index = buffer.index(b'\n')
                            response = buffer[:index].decode()
                            self.handle_response(response)
                            # Send one more.
                            cnt = self.sent_cnt
                            self.sent_cnt += 1
                            r = self.records[cnt]
                            self.record_status[int(r[0])] = (time.time_ns(), 0)
                            content = (",".join(r) +'\n').encode()
                            connection.sendall(content)
                            # remove from buffer.
                            buffer = buffer[index+1:]
                except socket.timeout:
                    continue
                except ConnectionResetError:
                    self.connections.remove(connection)
                    print(f"Connection broken with {self.ip_address}:{self.port}")
                    continue

    def handle_response(self, record):
        print(f"Received record: {record}")
        self.record_status[int(record)][1] = time.time_ns()

    def save_history(self, csv_file):
        # Save sending and feedback data along with the content sent to a new CSV file
        history_file = os.path.splitext(csv_file)[0] + "_history.csv"
        with open(history_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Sent Record", "Received Record"])
            for sent, received in zip(self.records_sent, self.records_received):
                writer.writerow([sent, received])

    def send_csv_files(self):
        csv_file = data["Generator"]["location"] + ".csv"
        self.connect_to_server()
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_file = os.path.join(script_dir, csv_file)

        # # Find the csv file that ends with the current IP address
        # self_ip = socket.gethostbyname(socket.gethostname())
        # self_csv_file = next((file for file in csv_files if file.endswith(f"_{self.ip_address}.csv")), None)

        # if self_csv_file is None:
        #     print(f"No CSV file found for IP {self.ip_address}")
        #     return
        self.send_records(csv_file)
        self.save_history(csv_file)


def generate_handler():
    print("Generating data points...")
    g = DataGenerator(data["Generator"]["count_millions"] * 1000000).generate_csv_records(data["Generator"]["location"] + ".csv")
    host_count, records_per_host, protocol_distribution = g.generate_statistics()
    g.save_statistics(data["Generator"]["location"] + ".txt", host_count, records_per_host, protocol_distribution)

def send_handler():
    threads = []
    assert(len(data["Sender"]["instances"]) == 1) # Only designed for 1 instance for now.
    for instance in data["Sender"]["instances"]:
        ip = instance["IP"]
        port = instance["port"]
        sender = Sender(ip, port)
        thread = threading.Thread(target=sender.send_csv_files)
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

def handle_sigint(signum, frame):
    print("Exiting gracefully...")
    # Add any cleanup code here if needed
    exit(0)

def main():
    signal.signal(signal.SIGINT, handle_sigint)
    if data["Command"] == "generate":
        generate_handler()
    else:
        send_handler()

main()