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
    keys = np.arange(0, num_keys)
    probabilities = 1 / np.power(keys + 1, zipf_skewness)  # keys + 1 to avoid division by zero
    probabilities /= np.sum(probabilities)  # Normalize to sum to 1
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
    types = np.random.choice(['Read', 'Write'], total_requests, p=[prob_read_write, 1 - prob_read_write])
    scopes = np.random.choice(['Per-flow', 'Cross-flow'], total_requests, p=[prob_scope, 1 - prob_scope])
    
    lines = []
    for i in range(total_requests):
        lines.append([i + 1, keys[i], vnfID, types[i], scopes[i]])
    
    return lines


def distribute_lines_among_instances(lines, instance_workloads, output_dir):
    """Distribute the generated lines among instances based on workload distribution."""
    os.makedirs(output_dir, exist_ok=True)
    
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


def generate_workload_with_keySkew(expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, locality, scopeRatio, puncInterval):
    rootDir = '/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/workload'
    workloadConfig = f'{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/scopeRatio={scopeRatio}'
    workloadDir = f'{rootDir}/{workloadConfig}'

    lines = generate_csv_lines(numPackets, numItems, keySkew, readRatio, scopeRatio, vnfID)

    instance_workloads = generate_workload_distribution(numInstances, numPackets, workloadSkew, puncInterval)
    distribute_lines_among_instances(lines, instance_workloads, workloadDir)
    print(f'Generated {expID} workload for keySkew={keySkew}, workloadSkew={workloadSkew}, readRatio={readRatio}, scopeRatio={scopeRatio}, locality={locality}')




def subtract_ranges(a, b, N):
    full_range = set(range(0, N))
    subtract_range = set(range(a, b))
    result = sorted(full_range - subtract_range)
    return result

def generate_workload_with_locality(expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, locality, scopeRatio, puncInterval):
    rootDir = '/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/workload'
    workloadConfig = f'{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/scopeRatio={scopeRatio}'
    workloadDir = f'{rootDir}/{workloadConfig}'

    readRatio = readRatio / 100
    locality = locality / 100
    scopeRatio = scopeRatio / 100

    # Ensure the output directory exists
    os.makedirs(workloadDir, exist_ok=True)
    
    for instance_id in range(numInstances):
        file_path = os.path.join(workloadDir, f'instance_{instance_id}.csv')
        
        # Check if the specific file exists and remove it if necessary
        if os.path.exists(file_path):
            os.remove(file_path)

        partition_start = instance_id * numItems // numInstances
        partition_end = (instance_id + 1) * numItems // numInstances
        intra_partition_keyset = list(range(partition_start, partition_end))
        cross_partition_keyset = subtract_ranges(partition_start, partition_end, numItems)
        num_request_instance = numPackets // numInstances

        # Write the new workload file
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for request_id in range(num_request_instance):
                random_locality = random.random()
                key = -1
                if (random_locality < locality):
                    key_index = random.randint(0, len(intra_partition_keyset) - 1)
                    key = intra_partition_keyset[key_index]
                else:
                    key_index = random.randint(0, len(cross_partition_keyset) - 1)
                    key = cross_partition_keyset[key_index]
                access_type = 'Read' if random.random() < readRatio else 'Write'
                scope = 'Per-flow' if random.random() < scopeRatio else 'Cross-flow'
                writer.writerow([request_id, key, vnfID, access_type, scope])
        
        print(f'Generated file: {file_path}')

# Common Parameters
puncInterval = 1000 # Used to normalize workload distribution among instances 
vnfID = 11
numPackets = 100000
numInstances = 4
numItems = 10000

# Per-phase Parameters

# Phase 1: Mostly per-flow, balanced / high skewness, read-write balanced
Phase1_expID = '5.2.2_phase1'
Phase1_keySkew = 0
Phase1_workloadSkew = 0
Phase1_readRatio = 50
Phase1_localityList = [75, 80, 85, 90, 95, 100]
Phase1_scopeRatio = 0

# Phase 2: Mostly cross-partition, balanced / high skewness, mostly read-only
Phase2_expID = '5.2.2_phase2'
Phase2_keySkewList = [0, 25, 50, 75, 100, 150]
Phase2_workloadSkew = 0
Phase2_readRatioList = [75, 80, 85, 90, 95, 100]
Phase2_locality = 0
Phase2_scopeRatio = 0

# Phase 3: Mostly cross-partition, balanced / high skewness, mostly write-only
Phase3_expID = '5.2.2_phase3'
Phase3_keySkewList = [0, 25, 50, 75, 100, 150]
Phase3_workloadSkew = 0
Phase3_readRatioList = [0, 5, 10, 15, 20, 25]
Phase3_locality = 0
Phase3_scopeRatio = 0

# Phase 4: Mostly cross-partition, high skewness, read-write balanced
Phase4_expID = '5.2.2_phase4'
Phase4_keySkewList = [0, 25, 50, 75, 100, 150]
Phase4_workloadSkew = 0
Phase4_readRatioList = [0, 25, 50, 75, 100]
Phase4_locality = 0
Phase4_scopeRatio = 0


# Workload generation
for Phase1_locality in Phase1_localityList:
    generate_workload_with_locality(Phase1_expID, vnfID, numPackets, numInstances, numItems, Phase1_keySkew, Phase1_workloadSkew, Phase1_readRatio, Phase1_locality, Phase1_scopeRatio, puncInterval)

for Phase2_keySkew in Phase2_keySkewList:
    for Phase2_readRatio in Phase2_readRatioList:
        generate_workload_with_keySkew(Phase2_expID, vnfID, numPackets, numInstances, numItems, Phase2_keySkew, Phase2_workloadSkew, Phase2_readRatio, Phase2_locality, Phase2_scopeRatio, puncInterval)

for Phase3_keySkew in Phase3_keySkewList:
    for Phase3_readRatio in Phase3_readRatioList:
        generate_workload_with_keySkew(Phase3_expID, vnfID, numPackets, numInstances, numItems, Phase3_keySkew, Phase3_workloadSkew, Phase3_readRatio, Phase3_locality, Phase3_scopeRatio, puncInterval)

for Phase4_keySkew in Phase4_keySkewList:
    for Phase4_readRatio in Phase4_readRatioList:
        generate_workload_with_keySkew(Phase4_expID, vnfID, numPackets, numInstances, numItems, Phase4_keySkew, Phase4_workloadSkew, Phase4_readRatio, Phase4_locality, Phase4_scopeRatio, puncInterval)
