import csv
import random
import os
import shutil

class PatternGenerator:
    def __init__(self, tuple_range=10000, instance_count=4, request_count=100000, pattern_1_prob=0.8, pattern_2_prob=1, pattern_3_prob=0.8):
        self.tuple_range = tuple_range
        self.instance_count = instance_count
        self.request_count = request_count
        self.base_dir = os.path.join(os.getcwd(), 'scripts/TransNFV/pattern_files/5.2.2')
        self.type_zero_probability = pattern_1_prob  # Probability for type 0 in sharedReaders pattern
        self.type_one_probability = pattern_2_prob    # Probability for type 1 in sharedWriters pattern
        self.type_two_probability = pattern_3_prob    # Probability for type 2 in mutualInteractive pattern

    def generate_files(self):
        # Remove existing files and directories before creating new ones
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
        os.makedirs(self.base_dir)


        # Define the patterns
        patterns = {
            'loneOperative': self.generate_lone_operative,
            'sharedReaders': self.generate_shared_readers,
            'sharedWriters': self.generate_shared_writers,
            'mutualInteractive': self.generate_mutual_interactive  # Placeholder for another pattern
        }

        for pattern_name, generation_method in patterns.items():
            pattern_dir = os.path.join(self.base_dir, f"instanceNum_{self.instance_count}", pattern_name)
            if not os.path.exists(pattern_dir):
                os.makedirs(pattern_dir)
            generation_method(pattern_dir)

    def generate_lone_operative(self, pattern_dir):
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
                    writer.writerow([request_id, tuple_id, type_id])
            print(f'Generated file: {file_name}')

    def generate_shared_readers(self, pattern_dir):
        for instance_index in range(self.instance_count):
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(0, self.tuple_range - 1)
                    # Generate type based on the class variable probability for type 0 (R)
                    type_id = 0 if random.random() < self.type_zero_probability else random.randint(1, 2)
                    writer.writerow([request_id, tuple_id, type_id])
            print(f'Generated file: {file_name}')

    def generate_shared_writers(self, pattern_dir):
        for instance_index in range(self.instance_count):
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(0, self.tuple_range - 1)
                    # Generate type based on the class variable probability for type 1 (W)
                    type_id = 1 if random.random() < self.type_one_probability else random.choice([0, 2])
                    writer.writerow([request_id, tuple_id, type_id])
            print(f'Generated file: {file_name}')

    def generate_mutual_interactive(self, pattern_dir):
        for instance_index in range(self.instance_count):
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(0, self.tuple_range - 1)
                    # Generate type based on the class variable probability for type 2 (R&W)
                    type_id = 2 if random.random() < self.type_two_probability else random.choice([0, 1])
                    writer.writerow([request_id, tuple_id, type_id])
            print(f'Generated file: {file_name}')

    def generate_random(self, pattern_dir):
        for instance_index in range(self.instance_count):
            file_name = os.path.join(pattern_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for request_id in range(self.request_count):
                    tuple_id = random.randint(0, self.tuple_range - 1)
                    type_id = random.randint(0, 2)
                    writer.writerow([request_id, tuple_id, type_id])
            print(f'Generated file: {file_name}')

# Usage
if __name__ == "__main__":
    # Example instantiation with a specific probability for type 0 in sharedReaders
    generator = PatternGenerator(pattern_1_prob=0.8)
    generator.generate_files()
