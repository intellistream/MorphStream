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

    # Generate keys in the range [0, num_keys-1]
    keys = np.arange(0, num_keys)

    # Calculate probabilities using Zipf's law
    probabilities = 1 / np.power(keys + 1, zipf_skewness)  # keys + 1 to avoid division by zero
    probabilities /= np.sum(probabilities)  # Normalize to sum to 1

    # Generate key accesses based on the Zipfian distribution
    key_accesses = np.random.choice(keys, size=num_samples, p=probabilities)

    return key_accesses


def generate_workload_distribution(num_instances, total_requests, workload_skewness, punc_interval):
    workload_skewness = workload_skewness / 100
    """Distribute workload across instances based on a skewed distribution."""
    short_requests = int(total_requests / (punc_interval * num_instances))
    if workload_skewness == 0:
        return np.full(num_instances, total_requests // num_instances, dtype=int)
    
    workload_distribution = zipfian_distribution(num_instances, workload_skewness, short_requests)
    requests_per_instance = np.bincount(workload_distribution, minlength=num_instances)
    print(requests_per_instance)
    large_requests_per_instance = [x * punc_interval * num_instances for x in requests_per_instance]
    print(large_requests_per_instance)

    return large_requests_per_instance


def generate_csv_lines(total_requests, num_keys, key_skewness, prob_read_write, prob_scope, vnfID):
    key_skewness = key_skewness / 100
    prob_read_write = prob_read_write / 100
    prob_scope = prob_scope / 100
    """Generate CSV lines based on the given skewness, read/write, and scope probabilities."""
    keys = zipfian_distribution(num_keys, key_skewness, total_requests)

    # Count the frequency of each key access
    key_counts = np.bincount(keys, minlength=num_keys)

    # Plot the frequency distribution
    # plt.figure(figsize=(10, 6))
    # plt.bar(range(num_keys), key_counts)
    # plt.xlabel('Key')
    # plt.ylabel('Frequency')
    # plt.title('Zipfian Distribution of Key Accesses')
    # plt.show()

    types = np.random.choice(['Read', 'Write'], total_requests, p=[prob_read_write, 1 - prob_read_write])
    scopes = np.random.choice(['Per-flow', 'Cross-flow'], total_requests, p=[prob_scope, 1 - prob_scope])
    
    lines = []
    for i in range(total_requests):
        lines.append([i + 1, keys[i], vnfID, types[i], scopes[i]])
    
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

def update_workload_file_path(indicator_path, workload_path):
    with open(indicator_path, 'w') as file:
        file.write(workload_path)

def generate_workload(expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, scopeRatio, locality, puncInterval):
    rootDir = '/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/workload'
    workloadConfig = f'{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/scopeRatio={scopeRatio}'
    workloadDir = f'{rootDir}/{workloadConfig}'

    # Generate all CSV lines
    lines = generate_csv_lines(numPackets, numItems, keySkew, readRatio, scopeRatio, vnfID)

    # Distribute lines based on workload skewness
    instance_workloads = generate_workload_distribution(numInstances, numPackets, workloadSkew, puncInterval)
    distribute_lines_among_instances(lines, instance_workloads, workloadDir)


# Example usage
puncInterval = 1000 # Used to normalize workload distribution among instances 
expID = '5.4.1'
vnfID = 11

numPackets = 400000
numInstances = 4
numItems = 10000

keySkewList = [160, 170, 180, 190]
workloadSkewList = [0, 25, 50, 75, 100]
readRatioList = [50]
scopeRatio = 0
locality = 0

for keySkew in keySkewList:
    for workloadSkew in workloadSkewList:
        for readRatio in readRatioList:
            generate_workload(expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, scopeRatio, locality, puncInterval)
            print(f'Generated {expID} workload for workloadSkew={workloadSkew}, readRatio={readRatio}')