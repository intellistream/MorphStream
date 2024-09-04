import csv
import random
import os
import numpy as np
import matplotlib.pyplot as plt

def subtract_ranges(a, b, N):
    full_range = set(range(0, N))
    subtract_range = set(range(a, b))
    result = sorted(full_range - subtract_range)
    return result

def generate_instance_workloads(root_path, num_instances, num_keys, total_requests, locality_prob, read_prob, per_flow_prob, vnfID):
    os.makedirs(root_path, exist_ok=True)
    for instance_id in range(num_instances):
        partition_start = instance_id * num_keys // num_instances
        partition_end = (instance_id + 1) * num_keys // num_instances
        intra_partition_keyset = list(range(partition_start, partition_end))
        cross_partition_keyset = subtract_ranges(partition_start, partition_end, num_keys)
        num_request_instance = total_requests // num_instances

        file_path = os.path.join(root_path, f'instance_{instance_id}.csv')

        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for request_id in range(num_request_instance):
                random_locality = random.random()
                key = -1
                if (random_locality < locality_prob):
                    key_index = random.randint(0, len(intra_partition_keyset) - 1)
                    key = intra_partition_keyset[key_index]
                else:
                    key_index = random.randint(0, len(cross_partition_keyset) - 1)
                    key = cross_partition_keyset[key_index]
                access_type = '0' if random.random() < read_prob else '1'
                scope = 'per-flow' if random.random() < per_flow_prob else 'cross-flow'
                writer.writerow([request_id, key, vnfID, access_type, scope])
            print("intra_partition_keyset: ", intra_partition_keyset[0], ", ", intra_partition_keyset[-1])
            print("cross_partition_keyset: ", cross_partition_keyset[0], ", ", cross_partition_keyset[-1])
        print(f'Generated file: {file_path}')


# Example usage
num_instances = 4
output_dir = f'/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/pattern_files/5.4.3/instanceNum_{num_instances}/dynamic'
num_keys = 10000
total_requests = 400000
key_skewness = 0  # Adjust this value from 0 (uniform) to 1 (highly skewed)
workload_skewness = 0  # Adjust this value from 0 (uniform) to 1 (highly skewed)
vnfID = 11
punc_interval = 1000

prob_locality = 0
prob_read_write = 0  # read / (read + write)
prob_scope = 0  # per-flow / (per-flow + cross-flow)

# Generate all CSV lines
generate_instance_workloads(output_dir, num_instances, num_keys, total_requests, prob_locality, prob_read_write, prob_scope, vnfID)