import argparse
import subprocess
import os
import threading
import pandas as pd
import matplotlib.pyplot as plt
import itertools

from pypmml import Model


def read_throughput(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy):
    throughput_file_path = f"{exp_dir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                  f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
                  f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
                  f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
                  f"doMVCC={doMVCC}/udfComplexity={udfComplexity}/throughput.csv"
    try:
        df = pd.read_csv(throughput_file_path, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
        return df['Throughput'].iloc[0]
    except Exception as e:
        print(f"Failed to read {throughput_file_path}: {e}")
        return None

vnfID = 11
numItems = 1000
numPackets = 400000
numInstances = 4
app = "nfv_test"
expID = '6_Training'


# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
doMVCC = 0
udfComplexity = 10
ccStrategy = "Offloading"

keySkewList = [0]
workloadSkewList = [0]
# readRatioList = [0, 50, 100]
# localityList = [0, 50, 100]
# scopeRatioList = [0, 50, 100]

# readRatioList = [0]
# localityList = [0, 50, 100]
# scopeRatioList = [0]

readRatioList = [0, 25, 50, 75, 100]
localityList = [0, 25, 50, 75, 100]
scopeRatioList = [0, 25, 50, 75, 100]


groundTruthLabelMap = {}

def generate_label(exp_dir):
    for keySkew, workloadSkew, readRatio, locality, scopeRatio in itertools.product(keySkewList, workloadSkewList, readRatioList, localityList, scopeRatioList):
        groundTruthLabelMap[(keySkew, workloadSkew, readRatio, locality, scopeRatio)] = f"Default"
        throughput = 0
        for ccStrategy in ["Offloading", "Replication", "Partitioning"]:
            currentThroughput = read_throughput(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy)
            if currentThroughput > throughput:
                throughput = currentThroughput
                groundTruthLabelMap[(keySkew, workloadSkew, readRatio, locality, scopeRatio)] = ccStrategy


def write_label_to_csv(exp_dir):
    label_file_path = f"{exp_dir}/training_data/optimal_modules.csv"
    with open(label_file_path, 'w') as f:
        for key, value in groundTruthLabelMap.items():
            f.write(f"{key[0]},{key[1]},{key[2]},{key[3]},{key[4]},{value}\n") # keySkew, workloadSkew, readRatio, locality, scopeRatio, optimal_strategy
    print(f"Ground truth labels written to {label_file_path}")



def main(root_dir, exp_dir):
    print(f"Root directory: {root_dir}")
    print(f"Experiment directory: {exp_dir}")
    generate_label(exp_dir)
    write_label_to_csv(exp_dir)

    print("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)
    print("Preliminary study results generated")