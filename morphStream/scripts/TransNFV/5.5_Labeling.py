import subprocess
import os
import time
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import csv
import itertools

def get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy):
    return f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                  f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
                  f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
                  f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
                  f"doMVCC={doMVCC}/udfComplexity={udfComplexity}/throughput.csv"


def determine_optimal_strategy(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, strategies, output_csv_path):
    optimal_results = []
    output_dir = os.path.dirname(output_csv_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Directory {output_dir} created.")

    if os.path.exists(output_csv_path):
        os.remove(output_csv_path)
        print(f"Existing file {output_csv_path} has been removed.")

    for strategy in strategies:
        throughput_file_path = get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, strategy)
        try:
            df = pd.read_csv(throughput_file_path, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
            throughput = df['Throughput'].iloc[0]
        except Exception as e:
            print(f"Error reading file {throughput_file_path}: {e}")
            continue
        
        optimal_results.append((strategy, throughput))
    
    if optimal_results:
        optimal_strategy = max(optimal_results, key=lambda x: x[1])[0]
        with open(output_csv_path, mode='a', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([keySkew, workloadSkew, readRatio, locality, scopeRatio, optimal_strategy])
        print(f"Optimal strategy written to {output_csv_path}")
    else:
        print(f"No valid throughput data found for workload: {keySkew, workloadSkew, readRatio, locality, scopeRatio}")




# Common Parameters
puncInterval = 1000 # Used to normalize workload distribution among instances 
vnfID = 11
numPackets = 100000
numInstances = 4
numItems = 10000
app = "nfv_test"

numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
ccStrategy = "Offloading"
doMVCC = 0
udfComplexity = 10
ccStrategyList = ["Partitioning", "Replication", "Offloading", "Proactive"]
rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"

# Per-phase Parameters

# Phase 1: Mostly per-flow, balanced / high skewness, read-write balanced
expID = '5.5'
Phase1_keySkewList = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
Phase1_workloadSkewList = [0]
Phase1_readRatioList = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
Phase1_localityList = [0]
Phase1_scopeRatioList = [0]

Phase2_keySkewList = [0]
Phase2_workloadSkew = [0]
Phase2_readRatioList = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
Phase2_localityList = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
Phase2_scopeRatio = [0]


def labelKeySkew():
    for keySkew in Phase1_keySkewList:
        for workloadSkew in Phase1_workloadSkewList:
            for readRatio in Phase1_readRatioList:
                for locality in Phase1_localityList:
                    for scopeRatio in Phase1_scopeRatioList:
                        determine_optimal_strategy(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategyList, f"{rootDir}/training_data/{expID}/optimal_strategies.csv")


if __name__ == "__main__":
    labelKeySkew()
    # localityExp()
    print("Done")