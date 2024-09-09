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

    # Ensure the output directory exists
    os.makedirs(workloadDir, exist_ok=True)
    
    # Remove all existing files in the output directory
    for filename in os.listdir(workloadDir):
        file_path = os.path.join(workloadDir, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            os.rmdir(file_path)

    os.makedirs(rootDir, exist_ok=True)
    for instance_id in range(numInstances):
        partition_start = instance_id * numItems // numInstances
        partition_end = (instance_id + 1) * numItems // numInstances
        intra_partition_keyset = list(range(partition_start, partition_end))
        cross_partition_keyset = subtract_ranges(partition_start, partition_end, numItems)
        num_request_instance = numPackets // numInstances

        file_path = os.path.join(workloadDir, f'instance_{instance_id}.csv')

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
            # print("intra_partition_keyset: ", intra_partition_keyset[0], ", ", intra_partition_keyset[-1])
            # print("cross_partition_keyset: ", cross_partition_keyset[0], ", ", cross_partition_keyset[-1])
        print(f'Generated file: {file_path}')


# Example usage
puncInterval = 1000 # Used to normalize workload distribution among instances 
expID = '5.4.3'
vnfID = 11

numPackets = 400000
numInstances = 4
numItems = 5000

keySkew = 0
workloadSkew = 0
readRatioList = [0, 25, 50, 75, 100]
scopeRatioList = [0, 25, 50, 75, 100]
localityList = [0, 25, 50, 75, 100]

for locality in localityList:
    for readRatio in readRatioList:
        for scopeRatio in scopeRatioList:
            generate_workload(expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, scopeRatio, locality, puncInterval)