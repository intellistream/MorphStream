import argparse
import subprocess
import os
import time
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import csv


def generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategyList,
                         doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio, script_path, root_dir):
    ccStrategyList_str = " ".join(map(str, ccStrategyList))
    script_content = f"""#!/bin/bash

function ResetParameters() {{
  app="{app}"
  expID="{expID}"
  vnfID="{vnfID}"
  nfvExperimentPath="{exp_dir}"
  numPackets={numPackets}
  numItems={numItems}
  numInstances={numInstances}
  numTPGThreads={numTPGThreads}
  numOffloadThreads={numOffloadThreads}
  puncInterval={puncInterval}
  ccStrategy="0"
  doMVCC={doMVCC}
  udfComplexity={udfComplexity}
  keySkew={keySkew}
  workloadSkew={workloadSkew}
  readRatio={readRatio}
  locality={locality}
  scopeRatio={scopeRatio}
}}

function runTStream() {{
  echo "java -Xms100g -Xmx100g -Xss10M -jar {root_dir}/morphStream/morph-clients/target/morph-clients-0.1.jar \\
          --app $app \\
          --expID $expID \\
          --vnfID $vnfID \\
          --nfvExperimentPath $nfvExperimentPath \\
          --numPackets $numPackets \\
          --numItems $numItems \\
          --numInstances $numInstances \\
          --numTPGThreads $numTPGThreads \\
          --numOffloadThreads $numOffloadThreads \\
          --puncInterval $puncInterval \\
          --ccStrategy $ccStrategy \\
          --doMVCC $doMVCC \\
          --udfComplexity $udfComplexity \\
          --keySkew $keySkew \\
          --workloadSkew $workloadSkew \\
          --readRatio $readRatio \\
          --locality $locality \\
          --scopeRatio $scopeRatio
          "
  java -Xms100g -Xmx100g -Xss10M -jar {root_dir}/morphStream/morph-clients/target/morph-clients-0.1.jar \\
    --app $app \\
    --expID $expID \\
    --vnfID $vnfID \\
    --nfvExperimentPath $nfvExperimentPath \\
    --numPackets $numPackets \\
    --numItems $numItems \\
    --numInstances $numInstances \\
    --numTPGThreads $numTPGThreads \\
    --numOffloadThreads $numOffloadThreads \\
    --puncInterval $puncInterval \\
    --ccStrategy $ccStrategy \\
    --doMVCC $doMVCC \\
    --udfComplexity $udfComplexity \\
    --keySkew $keySkew \\
    --workloadSkew $workloadSkew \\
    --readRatio $readRatio \\
    --locality $locality \\
    --scopeRatio $scopeRatio
}}

function iterateExperiments() {{
  ResetParameters
  ccStrategyList=({ccStrategyList_str})
  for scopeRatio in 0 25 50 75 100
  do
    for ccStrategy in "${{ccStrategyList[@]}}"
    do
      runTStream
    done
  done
}}

iterateExperiments
ResetParameters
"""

    with open(script_path, "w") as file:
        file.write(script_content)
    os.chmod(script_path, 0o755)

def stream_reader(pipe, pipe_name):
    with pipe:
        for line in iter(pipe.readline, ''):
            print(f"{pipe_name}: {line.strip()}")

def execute_bash_script(script_path):
    print(f"Executing bash script: {script_path}")

    process = subprocess.Popen(["bash", script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    stdout_thread = threading.Thread(target=stream_reader, args=(process.stdout, "STDOUT"))
    stderr_thread = threading.Thread(target=stream_reader, args=(process.stderr, "STDERR"))
    stdout_thread.start()
    stderr_thread.start()

    process.wait()
    stdout_thread.join()
    stderr_thread.join()

    if process.returncode != 0:
        print(f"Bash script finished with errors.")
    else:
        print(f"Bash script completed successfully.")


def plot_keyskew_throughput_figure(exp_dir, expID, vnfID, numPackets, numItems, numInstances,
                                   numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity,
                                   keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategyListFull,
                                   scopeRatioList, strategyInstanceMap, strategyOffloadExecutorMap):
    
    colors = ['white', 'white', 'white']
    hatches = ['\\\\\\', '////', 'xxx']
    hatch_colors = ['#8c0b0b', '#0060bf', '#129c03']

    data = {readRatioIndex: {} for readRatioIndex in scopeRatioList}

    for scopeRatioIndex in scopeRatioList:
        for ccStrategyIndex in ccStrategyListFull:
            numInstances = strategyInstanceMap.get(ccStrategyIndex, -1)
            numOffloadThreads = strategyOffloadExecutorMap.get(ccStrategyIndex, -1)

            outputFilePath = f"{exp_dir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/" \
                 f"scopeRatio={scopeRatioIndex}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategyIndex}/doMVCC={doMVCC}/udfComplexity={udfComplexity}/" \
                 "throughput.csv"

            try:
                df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                data[scopeRatioIndex][ccStrategyIndex] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                data[scopeRatioIndex][ccStrategyIndex] = None

    throughput_data = np.array([[data[localityIndex][ccStrategyIndex] if data[localityIndex][ccStrategyIndex] is not None else 0
                                 for ccStrategyIndex in ccStrategyListFull] for localityIndex in scopeRatioList]) / 1e6

    bar_width = 0.2
    index = np.arange(len(scopeRatioList))
    fig, ax = plt.subplots(figsize=(7, 4))

    displayedStrategyList = ["Partitioned", "Replicated", "Global"]
    for i, strategy in enumerate(ccStrategyListFull):
        ax.bar(index + i * bar_width, throughput_data[:, i], color=colors[i], hatch=hatches[i],
               edgecolor=hatch_colors[i], width=bar_width, label=displayedStrategyList[i])

    ax.set_xticks([r + bar_width for r in range(len(scopeRatioList))])
    ax.set_xticklabels(scopeRatioList, fontsize=16)
    ax.tick_params(axis='y', labelsize=14)
    ax.set_xlabel('Scope Ratio', fontsize=18)
    ax.set_ylabel('Throughput (Million req/sec)', fontsize=18)
    handles = [Patch(facecolor=color, edgecolor=hatchcolor, hatch=hatch, label=label)
               for color, hatchcolor, hatch, label in zip(colors, hatch_colors, hatches, displayedStrategyList)]
    ax.legend(handles=handles, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=3, fontsize=16)
    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)

    figure_name = f'{expID}_scopeRatio.pdf'
    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))
    plt.savefig(os.path.join(figure_dir, figure_name))


def plot_keyskew_latency_boxplot(exp_dir, expID, vnfID, numPackets, numItems, numInstances,
                                 numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity,
                                 keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategyListFull,
                                 scopeRatioList, strategyInstanceMap, strategyOffloadExecutorMap):
    
    data = {readRatioIndex: {ccStrategyIndex: [] for ccStrategyIndex in ccStrategyListFull} for readRatioIndex in scopeRatioList}
    
    for scopeRatioIndex in scopeRatioList:
        for ccStrategyIndex in ccStrategyListFull:
            numInstances = strategyInstanceMap.get(ccStrategyIndex, -1)
            numOffloadThreads = strategyOffloadExecutorMap.get(ccStrategyIndex, -1)

            outputFilePath = f"{exp_dir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/" \
                 f"scopeRatio={scopeRatioIndex}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategyIndex}/doMVCC={doMVCC}/udfComplexity={udfComplexity}/" \
                 "latency.csv"
            
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['latency'])
                data[scopeRatioIndex][ccStrategyIndex] = [value / 1e6 for value in df['latency'].tolist()]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                data[scopeRatioIndex][ccStrategyIndex] = []

    fig, ax = plt.subplots(figsize=(7, 4))
    
    boxplot_data = []
    boxplot_labels = []  # This will hold unique keySkew values
    colors = ['#8c0b0b', '#0060bf', '#129c03']
    displayedStrategyList = ["Partitioned", "Replicated", "Global"]
    
    positions = []  # Will store x-axis positions for the box plots
    num_cc_strategies = len(ccStrategyListFull)
    width_per_group = 0.8  # Space allocated per keySkew group

    for i, scopeRatioIndex in enumerate(scopeRatioList):
        for j, ccStrategyIndex in enumerate(ccStrategyListFull):
            latency_values = data[scopeRatioIndex][ccStrategyIndex]
            if latency_values:  # If there's data for this combination
                boxplot_data.append(latency_values)
                positions.append(i * (num_cc_strategies + 1) + j)
        
        boxplot_labels.append(f'{scopeRatioIndex}')

    bplot = ax.boxplot(boxplot_data, positions=positions, patch_artist=True, widths=0.6)
    for patch, color in zip(bplot['boxes'], colors * len(scopeRatioList)):
        patch.set_facecolor(color)

    for median in bplot['medians']:
        median.set(linewidth=2.5) # Set the median line width

    ax.set_xticks([i * (num_cc_strategies + 1) + num_cc_strategies / 2 - 0.5 for i in range(len(scopeRatioList))])
    ax.set_xticklabels(boxplot_labels, fontsize=16)
    ax.tick_params(axis='y', labelsize=16)
    
    ax.set_ylabel('Latency (s)', fontsize=18)
    ax.set_xlabel('Ratio of Per-flow State Access', fontsize=18)

    handles = [plt.Line2D([0], [0], color=color, lw=10) for color in colors]
    ax.legend(handles=handles, labels=displayedStrategyList, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=3, fontsize=16)
    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)

    figure_name_png = f'5.4.3_scopeRatio_lat.png'
    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name_png))




def runOffloading(root_dir, exp_dir):
    shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
    print(f"Shell script path: {shellScriptPath}")
    numInstances = strategyInstanceMap.get("Offloading", -1)
    numOffloadThreads = strategyOffloadExecutorMap.get("Offloading", -1)
    ccStrategyList = ["Offloading"]
    generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategyList, 
                         doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio, shellScriptPath, root_dir)
    
    execute_bash_script(shellScriptPath)


def runPATandREP(root_dir, exp_dir):
    shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
    print(f"Shell script path: {shellScriptPath}")
    numInstances = strategyInstanceMap.get("Partitioning", -1)
    numOffloadThreads = strategyOffloadExecutorMap.get("Partitioning", -1)
    ccStrategyList = ["Partitioning", "Replication"]
    generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategyList, 
                         doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio, shellScriptPath, root_dir)
    
    execute_bash_script(shellScriptPath)


# Basic params
app = "nfv_test"
expID = "5.4.3"
vnfID = 11
numItems = 1000
numPackets = 400000
numInstances = 4

# Workload chars
keySkew = 0
workloadSkew = 0
readRatio = 50
locality = 0
scopeRatio = 0

# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
ccStrategy = "Offloading"
doMVCC = 0
udfComplexity = 10
scopeRatioList = [0, 25, 50, 75, 100]
ccStrategyListFull = ["Partitioning", "Replication", "Offloading"]

strategyInstanceMap = {
    "Offloading": 4,
    "Partitioning": 8,
    "Replication": 8,
}
strategyOffloadExecutorMap = {
    "Offloading": 4,
    "Partitioning": 0,
    "Replication": 0,
}

def run_tradeoff(root_dir, exp_dir):
    runOffloading(root_dir, exp_dir)
    runPATandREP(root_dir, exp_dir)

def plot_throughput(exp_dir):
    plot_keyskew_throughput_figure(exp_dir, expID, vnfID, numPackets, numItems, numInstances,
                                   numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity,
                                   keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategyListFull, 
                                   scopeRatioList, strategyInstanceMap, strategyOffloadExecutorMap)

def plot_latency(exp_dir):
    plot_keyskew_latency_boxplot(exp_dir, expID, vnfID, numPackets, numItems, numInstances,
                                numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity, 
                                keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategyListFull,
                                scopeRatioList, strategyInstanceMap, strategyOffloadExecutorMap)

    print("Done")

def main(root_dir, exp_dir):

    print(f"Root directory: {root_dir}")
    print(f"Experiment directory: {exp_dir}")

    run_tradeoff(root_dir, exp_dir)
    plot_throughput(exp_dir)
    plot_latency(exp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)