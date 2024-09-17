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

def generate_workload(expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, scopeRatio, locality, puncInterval):
    rootDir = '/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/workload'
    workloadConfig = f'{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/scopeRatio={scopeRatio}'
    workloadDir = f'{rootDir}/{workloadConfig}'

    readRatio = readRatio / 100
    scopeRatio = scopeRatio / 100
    locality = locality / 100

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
                access_type = 'Read' if random.random() < readRatio else 'Read-Write'
                scope = 'Per-flow' if random.random() < scopeRatio else 'Cross-flow'
                writer.writerow([request_id, key, vnfID, access_type, scope])
        
        print(f'Generated file: {file_path}')


# Example usage
puncInterval = 1000 # Used to normalize workload distribution among instances 
expID = '5.5.1'
vnfID = 11

numPackets = 420000
numInstances = 4
numItems = 1200

keySkew = 0
workloadSkew = 0
readRatio = 50
scopeRatio = 0
locality = 0

for numInstances in [2, 4, 6, 8, 10]:
    generate_workload(expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, scopeRatio, locality, puncInterval)