import csv
import random
import os
import shutil

class PatternGenerator:
    def __init__(self, tuple_range=5000, instance_count=4, request_count=100000, pattern_prob=0.8):
        self.tuple_range = tuple_range
        self.instance_count = instance_count
        self.request_count = request_count
        self.base_dir = os.path.join(os.getcwd(), 'scripts/TransNFV/pattern_files/5.1')
        self.pattern_probability = pattern_prob  # Probability for type 0 in sharedReaders pattern

    def generate_files(self):
        # Remove existing files and directories before creating new ones
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
        os.makedirs(self.base_dir)

        # Define the patterns
        vnfs = {
            '1_firewall': self.generate_firewall,
            '2_nat': self.generate_nat,
            '3_lb': self.generate_lb,
            '4_trojan_detector': self.generate_trojan_detector,
            '5_portscan_detector': self.generate_portscan_detector,
            '6_prads': self.generate_prads,
            '7_session_border_controller': self.generate_session_border_controller,
            '8_ips': self.generate_ips,
            '9_squid': self.generate_squid,
            '10_adaptive_traffic_shaper': self.generate_adaptive_traffic_shaper
        }

        for vnf_name, generation_method in vnfs.items():
            pattern_dir = os.path.join(self.base_dir, f"instanceNum_{self.instance_count}", vnf_name)
            if not os.path.exists(pattern_dir):
                os.makedirs(pattern_dir)
            generation_method(pattern_dir)


    def generate_firewall(self, pattern_dir):
        partition_size = self.tuple_range // self.instance_count
        for instance_index in range(self.instance_count):
            min_range = instance_index * partition_size
            max_range = (instance_index + 1) * partition_size - 1
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(min_range, max_range)
                    type_id = random.randint(0, 2)
                    writer.writerow([request_id, tuple_id, 1, type_id])
            print(f'Generated file: {file_name}')

    def generate_nat(self, pattern_dir):
        partition_size = self.tuple_range // self.instance_count
        for instance_index in range(self.instance_count):
            min_range = instance_index * partition_size
            max_range = (instance_index + 1) * partition_size - 1
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(min_range, max_range)
                    type_id = 0 if random.random() < self.pattern_probability else 2
                    writer.writerow([request_id, tuple_id, 2, type_id])
            print(f'Generated file: {file_name}')

    def generate_lb(self, pattern_dir):
        partition_size = self.tuple_range // self.instance_count
        for instance_index in range(self.instance_count):
            min_range = instance_index * partition_size
            max_range = (instance_index + 1) * partition_size - 1
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(min_range, max_range)
                    type_id = 0 if random.random() < self.pattern_probability else 2
                    writer.writerow([request_id, tuple_id, 3, type_id])
            print(f'Generated file: {file_name}')

    def generate_trojan_detector(self, pattern_dir):
        partition_size = self.tuple_range // self.instance_count
        for instance_index in range(self.instance_count):
            min_range = instance_index * partition_size
            max_range = (instance_index + 1) * partition_size - 1
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(min_range, max_range)
                    type_id = 2
                    writer.writerow([request_id, tuple_id, 4, type_id])
            print(f'Generated file: {file_name}')

    def generate_portscan_detector(self, pattern_dir):
        partition_size = self.tuple_range // self.instance_count
        for instance_index in range(self.instance_count):
            min_range = instance_index * partition_size
            max_range = (instance_index + 1) * partition_size - 1
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(min_range, max_range)
                    type_id = 2
                    writer.writerow([request_id, tuple_id, 5, type_id])
            print(f'Generated file: {file_name}')

    def generate_prads(self, pattern_dir):
        partition_size = self.tuple_range // self.instance_count
        for instance_index in range(self.instance_count):
            min_range = instance_index * partition_size
            max_range = (instance_index + 1) * partition_size - 1
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(min_range, max_range)
                    type_id = 1 if random.random() < self.pattern_probability else 0
                    writer.writerow([request_id, tuple_id, 6, type_id])
            print(f'Generated file: {file_name}')

    def generate_session_border_controller(self, pattern_dir):
        partition_size = self.tuple_range // self.instance_count
        for instance_index in range(self.instance_count):
            min_range = instance_index * partition_size
            max_range = (instance_index + 1) * partition_size - 1
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(min_range, max_range)
                    type_id = 0 if random.random() < self.pattern_probability else 1
                    writer.writerow([request_id, tuple_id, 7, type_id])
            print(f'Generated file: {file_name}')

    def generate_ips(self, pattern_dir):
        partition_size = self.tuple_range // self.instance_count
        for instance_index in range(self.instance_count):
            min_range = instance_index * partition_size
            max_range = (instance_index + 1) * partition_size - 1
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(min_range, max_range)
                    type_id = 2 if random.random() < self.pattern_probability else 0
                    writer.writerow([request_id, tuple_id, 8, type_id])
            print(f'Generated file: {file_name}')

    def generate_squid(self, pattern_dir):
        partition_size = self.tuple_range // self.instance_count
        for instance_index in range(self.instance_count):
            min_range = instance_index * partition_size
            max_range = (instance_index + 1) * partition_size - 1
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(min_range, max_range)
                    type_id = 2
                    writer.writerow([request_id, tuple_id, 9, type_id])
            print(f'Generated file: {file_name}')

    def generate_adaptive_traffic_shaper(self, pattern_dir):
        partition_size = self.tuple_range // self.instance_count
        for instance_index in range(self.instance_count):
            min_range = instance_index * partition_size
            max_range = (instance_index + 1) * partition_size - 1
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(min_range, max_range)
                    type_id = 0 if random.random() < self.pattern_probability else 2
                    writer.writerow([request_id, tuple_id, 10, type_id])
            print(f'Generated file: {file_name}')

# Usage
if __name__ == "__main__":
    # Example instantiation with a specific probability for type 0 in sharedReaders
    generator = PatternGenerator()
    generator.generate_files()
