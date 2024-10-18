import argparse
import csv
import random
import os
import numpy as np
import matplotlib.pyplot as plt


def zipfian_distribution(num_keys, zipf_skewness, num_samples):
    keys = np.arange(0, num_keys)
    probabilities = 1 / np.power(keys + 1, zipf_skewness)  # keys + 1 to avoid division by zero
    probabilities /= np.sum(probabilities)  # Normalize to sum to 1
    key_accesses = np.random.choice(keys, size=num_samples, p=probabilities)

    return key_accesses


def generate_workload_distribution(num_instances, total_requests, workload_skewness, punc_interval):
    workload_skewness = workload_skewness / 100
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

    keys = zipfian_distribution(num_keys, key_skewness, total_requests)
    types = np.random.choice(['Read', 'Write'], total_requests, p=[prob_read_write, 1 - prob_read_write])
    scopes = np.random.choice(['Per-flow', 'Cross-flow'], total_requests, p=[prob_scope, 1 - prob_scope])

    lines = []
    for i in range(total_requests):
        lines.append([i + 1, keys[i], vnfID, types[i], scopes[i]])

    return lines


def distribute_lines_among_instances(lines, instance_workloads, output_dir):
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


def generate_workload_with_keySkew(expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio,
                                   locality, scopeRatio, puncInterval, exp_dir):
    rootDir = f'{exp_dir}/workload'
    workloadConfig = f'{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/scopeRatio={scopeRatio}'
    workloadDir = f'{rootDir}/{workloadConfig}'

    lines = generate_csv_lines(numPackets, numItems, keySkew, readRatio, scopeRatio, vnfID)

    instance_workloads = generate_workload_distribution(numInstances, numPackets, workloadSkew, puncInterval)
    distribute_lines_among_instances(lines, instance_workloads, workloadDir)
    print(
        f'Generated {expID} workload for keySkew={keySkew}, workloadSkew={workloadSkew}, readRatio={readRatio}, scopeRatio={scopeRatio}, locality={locality}')


def subtract_ranges(a, b, N):
    full_range = set(range(0, N))
    subtract_range = set(range(a, b))
    result = sorted(full_range - subtract_range)
    return result


def generate_workload_with_locality(expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio,
                                    locality, scopeRatio, puncInterval, exp_dir):
    rootDir = f'{exp_dir}/workload'
    workloadConfig = f'{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/scopeRatio={scopeRatio}'
    workloadDir = f'{rootDir}/{workloadConfig}'

    readRatio = readRatio / 100
    locality = locality / 100
    scopeRatio = scopeRatio / 100

    os.makedirs(workloadDir, exist_ok=True)

    for instance_id in range(numInstances):
        file_path = os.path.join(workloadDir, f'instance_{instance_id}.csv')

        if os.path.exists(file_path):
            os.remove(file_path)

        partition_start = instance_id * numItems // numInstances
        partition_end = (instance_id + 1) * numItems // numInstances
        intra_partition_keyset = list(range(partition_start, partition_end))
        cross_partition_keyset = subtract_ranges(partition_start, partition_end, numItems)
        num_request_instance = numPackets // numInstances

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


puncInterval = 1000  # Used to normalize workload distribution among instances
numPackets = 400000
numInstances = 4
numItems = 10000
expID = "5.2.1"

# KeySkew, WorkloadSkew, ReadRatio, Locality, ScopeRatio
workload1 = [0, 0, 50, 100, 100] # FW
comp1 = 1
workload2 = [0, 0, 80, 0, 50] # NAT
comp2 = 2
workload3 = [50, 0, 50, 0, 0] # LB
comp3 = 4
workload4 = [0, 0, 50, 0, 20] # TD
comp4 = 10
workload5 = [0, 0, 50, 0, 0] # PD
comp5 = 6
workload6 = [0, 0, 50, 0, 0] # PRADS
comp6 = 2
workload7 = [0, 0, 60, 0, 0] # SBC
comp7 = 4
workload8 = [0, 0, 60, 0, 0] # IPS
comp8 = 5
workload9 = [0, 0, 0, 0, 0] # SCP
comp9 = 3
workload10 = [0, 0, 60, 0, 0] # ATS
comp10 = 5

workloadList = [
    workload1, workload2, workload3, workload4, workload5, workload6, workload7, workload8, workload9, workload10
]


def generate(exp_dir):
    for i in range(0, 1):
        workload = workloadList[i]
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload
        generate_workload_with_locality(expID, i+1, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio,
                                        locality, scopeRatio, puncInterval, exp_dir)

    for i in range(1, 10):
        workload = workloadList[i]
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload
        generate_workload_with_keySkew(expID, i+1, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio,
                                       locality, scopeRatio, puncInterval, exp_dir)


def main(root_dir, exp_dir):

    print(f"Root directory: {root_dir}")
    print(f"Experiment directory: {exp_dir}")

    generate(exp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)