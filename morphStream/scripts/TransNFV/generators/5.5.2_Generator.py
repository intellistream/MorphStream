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


def generate_workload_with_keySkew(exp_dir, expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, locality, scopeRatio, puncInterval):
    exp_dir = f'{exp_dir}/workload'
    # rootDir = '/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/workload'
    workloadConfig = f'temporary/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/scopeRatio={scopeRatio}'
    workloadDir = f'{exp_dir}/{workloadConfig}'

    lines = generate_csv_lines(numPackets, numItems, keySkew, readRatio, scopeRatio, vnfID)

    instance_workloads = generate_workload_distribution(numInstances, numPackets, workloadSkew, puncInterval)
    distribute_lines_among_instances(lines, instance_workloads, workloadDir)
    print(f'Generated {expID} workload for keySkew={keySkew}, workloadSkew={workloadSkew}, readRatio={readRatio}, scopeRatio={scopeRatio}, locality={locality}')




def subtract_ranges(a, b, N):
    full_range = set(range(0, N))
    subtract_range = set(range(a, b))
    result = sorted(full_range - subtract_range)
    return result

def generate_workload_with_locality(exp_dir, expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, locality, scopeRatio, puncInterval):
    exp_dir = f'{exp_dir}/workload'
    # rootDir = '/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/workload'
    workloadConfig = f'temporary/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/scopeRatio={scopeRatio}'
    workloadDir = f'{exp_dir}/{workloadConfig}'

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


def combine_and_randomize_csv(csv_file_list, output_file):
    combined_lines = []

    # Read all CSV files and combine lines
    for file in csv_file_list:
        if os.path.isfile(file):  # Ensure it's a valid file
            with open(file, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    combined_lines.append(row)
        else:
            print(f"Warning: {file} is not a valid file or does not exist.")

    # Randomize the combined lines
    random.shuffle(combined_lines)

    # Write the shuffled lines to the output CSV file
    with open(output_file, 'w', newline='') as f_out:
        writer = csv.writer(f_out)
        writer.writerows(combined_lines)


def combine_csv_randomly(combinedWorkloadPath, rootDir):
    rootDir = f'{rootDir}/workload'
    for workloadInterval in workloadIntervalList:
        perIntervalPath = f'{combinedWorkloadPath}/interval={workloadInterval}' # Each interval has its own file
        os.makedirs(perIntervalPath, exist_ok=True)
        
        for instanceID in range(numInstances):
            per_interval_per_instance_file_path = f'{perIntervalPath}/instance_{instanceID}.csv'
            if os.path.exists(per_interval_per_instance_file_path):
                os.remove(per_interval_per_instance_file_path)

            per_interval_per_instance_files = []
            num_phases = numPackets // workloadInterval

            for phaseID in range(num_phases):
                expID = f'5.5.2_Interval{workloadInterval}_Phase{phaseID}'

                if phaseID % 2 == 0:
                    workloadFilePath = f'{rootDir}/temporary/{expID}/vnfID={vnfID}/numPackets={workloadInterval}/' \
                    f'numInstances={numInstances}/numItems={numItems}/keySkew={Partition_keySkew}/workloadSkew={Partition_workloadSkew}/' \
                    f'readRatio={Partition_readRatio}/locality={Partition_locality}/scopeRatio={Partition_scopeRatio}'
                else:
                    workloadFilePath = f'{rootDir}/temporary/{expID}/vnfID={vnfID}/numPackets={workloadInterval}/' \
                    f'numInstances={numInstances}/numItems={numItems}/keySkew={Offloading_keySkew}/workloadSkew={Offloading_workloadSkew}/' \
                    f'readRatio={Offloading_readRatio}/locality={Offloading_locality}/scopeRatio={Offloading_scopeRatio}'
                
                file_path = os.path.join(workloadFilePath, f'instance_{instanceID}.csv')
                per_interval_per_instance_files.append(file_path)

            combine_and_randomize_csv(per_interval_per_instance_files, per_interval_per_instance_file_path)


# Common Parameters
puncInterval = 1000 # Used to normalize workload distribution among instances 
vnfID = 11
# numPackets = 800000
# workloadIntervalList = [10000, 20000, 50000, 100000, 200000]
numPackets = 800000
workloadIntervalList = [20000, 40000, 100000, 200000, 400000]
numInstances = 4
numItems = 100

defaultKeySkew = 0
defaultWorkloadSkew = 0
defaultReadRatio = 0
defaultLocality = 0
defaultScopeRatio = 0

expID = '5.5.2'


Partition_keySkew = 0
Partition_workloadSkew = 0
Partition_readRatio = 0
Partition_locality = 100
Partition_scopeRatio = 0

Offloading_keySkew = 50
Offloading_workloadSkew = 0
Offloading_readRatio = 50
Offloading_locality = 0
Offloading_scopeRatio = 0

def generate(exp_dir):
    expID = '5.5.2'
    combinedWorkloadPath = f'{exp_dir}/workload/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/numItems={numItems}/' \
                           f'keySkew={defaultKeySkew}/workloadSkew={defaultWorkloadSkew}/readRatio={defaultReadRatio}/locality={defaultLocality}/scopeRatio={defaultScopeRatio}'
    for workloadInterval in workloadIntervalList:
        num_phases = numPackets // workloadInterval
        for phase in range(num_phases):
            if (phase % 2 == 0):
                expID = f'5.5.2_Interval{workloadInterval}_Phase{phase}'
                generate_workload_with_locality(exp_dir, expID, vnfID, workloadInterval, numInstances, numItems, Partition_keySkew, Partition_workloadSkew, 
                                                Partition_readRatio, Partition_locality, Partition_scopeRatio, puncInterval)

            else:
                expID = f'5.5.2_Interval{workloadInterval}_Phase{phase}'
                generate_workload_with_keySkew(exp_dir, expID, vnfID, workloadInterval, numInstances, numItems, Offloading_keySkew, Offloading_workloadSkew, 
                                                Offloading_readRatio, Offloading_locality, Offloading_scopeRatio, puncInterval)
    
    
    combine_csv_randomly(combinedWorkloadPath, exp_dir)


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