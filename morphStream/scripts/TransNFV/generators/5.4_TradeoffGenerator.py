import csv
import random
import os
import shutil
import numpy as np

# Control workload skewness, returns the number of packets per instance
def generate_workload_distribution(num_instances, total_requests, workload_skewness):
    # Generate skewed distribution for workload
    workload_distribution = np.random.zipf(workload_skewness, num_instances)
    workload_distribution = workload_distribution / workload_distribution.sum()  # Normalize to sum to 1
    instance_workloads = (workload_distribution * total_requests).astype(int)
    
    # Adjust to ensure the total workload matches the total_requests
    while instance_workloads.sum() < total_requests:
        instance_workloads[np.argmax(instance_workloads)] += 1
    
    return instance_workloads


#TODO: Control skewness for keys in a global scale: If a key is popular, it should be popular in all instances, not just one.
def generate_zipfian_keys(num_keys, num_requests, skewness):
    # Generate keys using Zipfian distribution
    if skewness == 0:
        return np.random.choice(np.arange(num_keys), num_requests, replace=True)
    else:
        keys = np.random.zipf(skewness, num_requests) - 1
        keys = keys % num_keys  # Ensure keys are within the range [0, num_keys-1]
        return keys


def generate_partitioned_keys(num_requests, intra_partition_prob, partition_keys, other_keys, intra_skewness, inter_skewness):
    # Decide if each request is in-partition or out-of-partition
    in_partition_mask = np.random.rand(num_requests) < intra_partition_prob
    
    # Generate keys based on the skewness within each partition
    in_partition_keys = generate_skewed_keys(len(partition_keys), in_partition_mask.sum(), intra_skewness)
    out_partition_keys = generate_skewed_keys(len(other_keys), (~in_partition_mask).sum(), inter_skewness)
    
    # Map the keys back to the actual key indices
    in_partition_keys = [partition_keys[k] for k in in_partition_keys]
    out_partition_keys = [other_keys[k] for k in out_partition_keys]
    
    # Combine the keys, respecting the in/out partition decision
    keys = np.empty(num_requests, dtype=int)
    keys[in_partition_mask] = in_partition_keys
    keys[~in_partition_mask] = out_partition_keys
    
    return keys


def generate_csv_files(num_instances, num_keys, total_requests, key_skewness, workload_skewness, prob_read_write, prob_scope):
    instance_workloads = generate_workload_distribution(num_instances, total_requests, workload_skewness)
    
    for instance_id in range(num_instances):
        num_requests = instance_workloads[instance_id]
        keys = generate_zipfian_keys(num_keys, num_requests, key_skewness)
        
        types = np.random.choice(['read', 'write'], num_requests, p=[prob_read_write, 1 - prob_read_write])
        scopes = np.random.choice(['per-flow', 'cross-flow'], num_requests, p=[prob_scope, 1 - prob_scope])
        
        with open(f'instance_{instance_id}.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for i in range(num_requests):
                writer.writerow([i + 1, keys[i], types[i], scopes[i]])

# Example usage
num_instances = 5
num_keys = 1000
total_requests = 10000
key_skewness = 1.0
workload_skewness = 1.5  # Adjust this value for different workload skews
prob_read_write = 0.7  # 70% read, 30% write
prob_scope = 0.6  # 60% per-flow, 40% cross-flow

generate_csv_files(num_instances, num_keys, total_requests, key_skewness, workload_skewness, prob_read_write, prob_scope)