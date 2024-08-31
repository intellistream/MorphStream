import csv
import random
import os
import numpy as np
import matplotlib.pyplot as plt


def zipfian_distribution(num_keys, zipf_skewness, num_samples):
    """
    Simulates a Zipfian distribution with adjustable skewness factor.

    Parameters:
    num_keys (int): Number of unique keys in the state table.
    zipf_skewness (float): The skewness factor of the Zipfian distribution.
    num_samples (int): Number of key accesses to simulate.

    Returns:
    key_accesses (list): List of accessed keys.
    """

    # Generate keys in the range [1, num_keys]
    keys = np.arange(1, num_keys + 1)

    # Calculate probabilities using Zipf's law
    probabilities = 1 / np.power(keys, zipf_skewness)
    probabilities /= np.sum(probabilities)  # Normalize to sum to 1

    # Generate key accesses based on the Zipfian distribution
    key_accesses = np.random.choice(keys, size=num_samples, p=probabilities)

    return key_accesses


def generate_workload_distribution(num_instances, total_requests, workload_skewness):
    """Distribute workload across instances based on a skewed distribution."""
    if workload_skewness == 0:
        return np.full(num_instances, total_requests // num_instances, dtype=int)
    
    workload_distribution = zipfian_distribution(num_instances, workload_skewness, total_requests)
    requests_per_instance = np.bincount(workload_distribution, minlength=num_instances + 1)[1:]
    print(requests_per_instance)

    return requests_per_instance

def generate_csv_lines(total_requests, num_keys, key_skewness, prob_read_write, prob_scope):
    """Generate CSV lines based on the given skewness, read/write, and scope probabilities."""
    keys = zipfian_distribution(num_keys, key_skewness, total_requests)

    # Count the frequency of each key access
    key_counts = np.bincount(keys, minlength=num_keys + 1)[1:]

    # Plot the frequency distribution
    plt.figure(figsize=(10, 6))
    plt.bar(range(1, num_keys + 1), key_counts)
    plt.xlabel('Key')
    plt.ylabel('Frequency')
    plt.title('Zipfian Distribution of Key Accesses')
    plt.show()

    types = np.random.choice(['read', 'write'], total_requests, p=[prob_read_write, 1 - prob_read_write])
    scopes = np.random.choice(['per-flow', 'cross-flow'], total_requests, p=[prob_scope, 1 - prob_scope])
    
    lines = []
    for i in range(total_requests):
        lines.append([i + 1, keys[i], types[i], scopes[i]])
    
    return lines

def distribute_lines_among_instances(lines, instance_workloads, output_dir):
    """Distribute the generated lines among instances based on workload distribution."""
    
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Remove all existing files in the output directory
    for filename in os.listdir(output_dir):
        file_path = os.path.join(output_dir, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            os.rmdir(file_path)
    
    start_idx = 0
    for instance_id, workload in enumerate(instance_workloads):
        end_idx = start_idx + workload
        instance_lines = lines[start_idx:end_idx]
        start_idx = end_idx
        
        file_path = os.path.join(output_dir, f'instance_{instance_id}.csv')
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(instance_lines)


# Example usage
num_instances = 4
output_dir = f'/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/pattern_files/5.4.1/instanceNum_{num_instances}/dynamic'
num_keys = 1000
total_requests = 10000
key_skewness = 0.5  # Adjust this value from 0 (uniform) to 1 (highly skewed)
workload_skewness = 0.6  # Adjust this value from 0 (uniform) to 1 (highly skewed)
prob_read_write = 0.7  # 70% read, 30% write
prob_scope = 0.6  # 60% per-flow, 40% cross-flow

# Generate all CSV lines
lines = generate_csv_lines(total_requests, num_keys, key_skewness, prob_read_write, prob_scope)

# Distribute lines based on workload skewness
instance_workloads = generate_workload_distribution(num_instances, total_requests, workload_skewness)
distribute_lines_among_instances(lines, instance_workloads, output_dir)