import csv
import random
import os
import shutil

class PatternGenerator:
    def __init__(self, tuple_range=10000, instance_count=4, request_count=10000, pattern_1_prob=0.8, pattern_2_prob=1, pattern_3_prob=0.8):
        self.tuple_range = tuple_range
        self.instance_count = instance_count
        self.request_count = request_count
        self.base_dir = os.path.join(os.getcwd(), 'scripts/TransNFV/pattern_files/5.2.2/' + f"instanceNum_{self.instance_count}" + "/dynamic/")
        self.type_zero_probability = pattern_1_prob  # Probability for type 0 in sharedReaders pattern
        self.type_one_probability = pattern_2_prob    # Probability for type 1 in sharedWriters pattern
        self.type_two_probability = pattern_3_prob    # Probability for type 2 in mutualInteractive pattern

    def generate_files(self):
        # Remove existing files and directories before creating new ones
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
        os.makedirs(self.base_dir)

        for instance_index in range(self.instance_count):
            # file_name = os.path.join(self.base_dir, f"instanceNum_{self.instance_count}", "dynamic", f'instance_{instance_index}.csv')
            file_name = os.path.join(self.base_dir, f'instance_{instance_index}.csv')
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)

                # Generate data for each pattern in sequence
                self.generate_lone_operative(writer, instance_index)
                self.generate_shared_writers(writer, instance_index)
                self.generate_shared_readers(writer, instance_index)
                self.generate_mutual_interactive(writer, instance_index)

            print(f'Generated file: {file_name}')

    def generate_lone_operative(self, writer, instance_index):
        partition_size = self.tuple_range // self.instance_count
        min_range = instance_index * partition_size
        max_range = (instance_index + 1) * partition_size - 1
        for request_id in range(self.request_count):
            tuple_id = random.randint(min_range, max_range)
            type_id = random.randint(0, 2)
            writer.writerow([request_id, tuple_id, type_id, 'loneOperative'])

    def generate_shared_readers(self, writer, instance_index):
        for request_id in range(self.request_count):
            tuple_id = random.randint(0, self.tuple_range - 1)
            # Generate type based on the class variable probability for type 0 (R)
            type_id = 0 if random.random() < self.type_zero_probability else random.randint(1, 2)
            writer.writerow([request_id, tuple_id, type_id, 'sharedReaders'])

    def generate_shared_writers(self, writer, instance_index):
        for request_id in range(self.request_count):
            tuple_id = random.randint(0, self.tuple_range - 1)
            # Generate type based on the class variable probability for type 1 (W)
            type_id = 1 if random.random() < self.type_one_probability else random.choice([0, 2])
            writer.writerow([request_id, tuple_id, type_id, 'sharedWriters'])

    def generate_mutual_interactive(self, writer, instance_index):
        for request_id in range(self.request_count):
            tuple_id = random.randint(0, self.tuple_range - 1)
            # Generate type based on the class variable probability for type 2 (R&W)
            type_id = 2 if random.random() < self.type_two_probability else random.choice([0, 1])
            writer.writerow([request_id, tuple_id, type_id, 'mutualInteractive'])

# Usage
if __name__ == "__main__":
    generator = PatternGenerator(pattern_1_prob=0.8)
    generator.generate_files()
