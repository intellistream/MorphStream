import csv
import random
import os
import shutil

class PatternGenerator:

    def __init__(self, tuple_range=10000, instance_count=4, request_count=100000,
                 pattern_1_prob=0.8, pattern_2_prob=1, pattern_3_prob=0.8):
        self.tuple_range = tuple_range
        self.instance_count = instance_count
        self.request_count = request_count
        self.base_dir = os.path.join(os.getcwd(), 'scripts/TransNFV/pattern_files/5.3.2/' + f"instanceNum_{self.instance_count}" + "/dynamic")
        self.sr_prob = pattern_1_prob  # Probability for type 0 in sharedReaders pattern
        self.sw_prob = pattern_2_prob    # Probability for type 1 in sharedWriters pattern
        self.mi_prob = pattern_3_prob    # Probability for type 2 in mutualInteractive pattern
        self.new_pattern_start_percentile = 0.2
        self.new_pattern_end_percentile_list = [0.4, 0.6, 0.8]

    def generate_files(self):
        # Remove existing files and directories before creating new ones
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
        os.makedirs(self.base_dir)

        for end_percentile in self.new_pattern_end_percentile_list:
            old_pattern_phase1_num = int(self.request_count * self.new_pattern_start_percentile)
            new_pattern_num = int(self.request_count * (end_percentile - self.new_pattern_start_percentile))
            old_pattern_phase2_num = self.request_count - old_pattern_phase1_num - new_pattern_num

            for instance_index in range(self.instance_count):
                dir_path = os.path.join(self.base_dir, "study_" + str(end_percentile))
                if not os.path.exists(dir_path):
                    os.makedirs(dir_path)
                file_name = os.path.join(dir_path, f'instance_{instance_index}.csv')
                with open(file_name, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    self.generate_lone_operative(writer, instance_index, old_pattern_phase1_num)
                    self.generate_shared_readers(writer, instance_index, new_pattern_num)
                    self.generate_lone_operative(writer, instance_index, old_pattern_phase2_num)

                print(f'Generated file: {file_name}')

    def generate_lone_operative(self, writer, instance_index, request_count):
        partition_size = self.tuple_range // self.instance_count
        min_range = instance_index * partition_size
        max_range = (instance_index + 1) * partition_size - 1
        for request_id in range(request_count):
            tuple_id = random.randint(min_range, max_range)
            type_id = random.randint(0, 2)
            writer.writerow([request_id, tuple_id, 1, type_id])

    def generate_shared_readers(self, writer, instance_index, request_count):
        for request_id in range(request_count):
            tuple_id = random.randint(0, self.tuple_range - 1)
            # Generate type based on the class variable probability for type 0 (R)
            type_id = 0 if random.random() < self.sr_prob else random.randint(1, 2)
            writer.writerow([request_id, tuple_id, 1, type_id])

    def generate_shared_writers(self, writer, instance_index, request_count):
        for request_id in range(request_count):
            tuple_id = random.randint(0, self.tuple_range - 1)
            # Generate type based on the class variable probability for type 1 (W)
            type_id = 1 if random.random() < self.sw_prob else random.choice([0, 2])
            writer.writerow([request_id, tuple_id, 1, type_id])

    def generate_mutual_interactive(self, writer, instance_index, request_count):
        for request_id in range(request_count):
            tuple_id = random.randint(0, self.tuple_range - 1)
            # Generate type based on the class variable probability for type 2 (R&W)
            type_id = 2 if random.random() < self.mi_prob else random.choice([0, 1])
            writer.writerow([request_id, tuple_id, 1, type_id])

# Usage
if __name__ == "__main__":
    generator = PatternGenerator()
    generator.generate_files()
