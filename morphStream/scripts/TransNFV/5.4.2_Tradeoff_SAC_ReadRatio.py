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
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy,
                         doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio, script_path,
                         gcCheckInterval, gcBatchInterval, root_dir):
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
  ccStrategy="{ccStrategy}"
  doMVCC={doMVCC}
  udfComplexity={udfComplexity}
  keySkew={keySkew}
  workloadSkew={workloadSkew}
  readRatio={readRatio}
  locality={locality}
  scopeRatio={scopeRatio}
  gcCheckInterval={gcCheckInterval}
  gcBatchInterval={gcBatchInterval}
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
          --scopeRatio $scopeRatio \\
          --gcCheckInterval $gcCheckInterval \\
          --gcBatchInterval $gcBatchInterval
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
    --scopeRatio $scopeRatio \\
    --gcCheckInterval $gcCheckInterval \\
    --gcBatchInterval $gcBatchInterval
}}

function iterateExperiments() {{
  ResetParameters
  for readRatio in 0 25 50 75 100
  do
    for doMVCC in 0 1
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

    # Make the script executable
    os.chmod(script_path, 0o755)

def stream_reader(pipe, pipe_name):
    with pipe:
        for line in iter(pipe.readline, ''):
            print(f"{pipe_name}: {line.strip()}")

def execute_bash_script(script_path):
    print(f"Executing bash script: {script_path}")

    # Execute the bash script
    process = subprocess.Popen(["bash", script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Start threads to read stdout and stderr
    stdout_thread = threading.Thread(target=stream_reader, args=(process.stdout, "STDOUT"))
    stderr_thread = threading.Thread(target=stream_reader, args=(process.stderr, "STDERR"))
    stdout_thread.start()
    stderr_thread.start()

    # Wait for the process to complete
    process.wait()
    stdout_thread.join()
    stderr_thread.join()

    if process.returncode != 0:
        print(f"Bash script finished with errors.")
    else:
        print(f"Bash script completed successfully.")


def plot_keyskew_throughput_figure(nfvExperimentPath,
                        expID, vnfID, numPackets, numItems, numInstances, 
                        numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity, 
                        keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy,
                        readRatioList, sacList):
    
    colors = ['white', 'white']
    hatches = ['\\\\\\', '////']
    hatch_colors = ['#0060bf', '#8c0b0b']

    # Prepare the structure to hold data
    data = {readRatioIndex: {} for readRatioIndex in readRatioList}

    # Iterate over the patterns and ccStrategies
    for readRatioIndex in readRatioList:
        for sacOptionIndex in sacList:
            outputFilePath = f"{nfvExperimentPath}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatioIndex}/locality={locality}/" \
                 f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategy}/doMVCC={sacOptionIndex}/udfComplexity={udfComplexity}/" \
                 "throughput.csv"

            # Read the CSV file
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                data[readRatioIndex][sacOptionIndex] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                data[readRatioIndex][sacOptionIndex] = None
    throughput_data = np.array([[data[readRatioIndex][sacOptionIndex] if data[readRatioIndex][sacOptionIndex] is not None else 0
                                 for sacOptionIndex in sacList] for readRatioIndex in readRatioList]) / 1e6
    bar_width = 0.2
    index = np.arange(len(readRatioList))
    fig, ax = plt.subplots(figsize=(7, 4))

    displayedStrategyList = ["SVCC", "MVCC"]
    for i, strategy in enumerate(sacList):
        ax.bar(index + i * bar_width, throughput_data[:, i], color=colors[i], hatch=hatches[i],
               edgecolor=hatch_colors[i], width=bar_width, label=displayedStrategyList[i])

    ax.set_xticks([r + bar_width for r in range(len(readRatioList))])
    ax.set_xticklabels(readRatioList, fontsize=16)
    ax.tick_params(axis='y', labelsize=14)

    ax.set_xlabel('Read Ratio', fontsize=18)
    ax.set_ylabel('Throughput (Million req/sec)', fontsize=18)
    handles = [Patch(facecolor=color, edgecolor=hatchcolor, hatch=hatch, label=label)
               for color, hatchcolor, hatch, label in zip(colors, hatch_colors, hatches, displayedStrategyList)]
    ax.legend(handles=handles, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=2, fontsize=16)

    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)

    figure_name = f'{expID}_readRatio.pdf'
    figure_dir = os.path.join(nfvExperimentPath, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))  # Save the figure
    plt.savefig(os.path.join(figure_dir, figure_name))  # Save the figure



def plot_keyskew_latency_boxplot(nfvExperimentPath,
                                 expID, vnfID, numPackets, numItems, numInstances, numTPGThreads, numOffloadThreads, 
                                 puncInterval, doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, 
                                 scopeRatio, ccStrategy,
                                 readRatioList, doMVCCList):
    
    data = {readRatioIndex: {doMVCCIndex: [] for doMVCCIndex in doMVCCList} for readRatioIndex in readRatioList}
    
    for readRatioIndex in readRatioList:
        for doMVCCIndex in doMVCCList:
            outputFilePath = f"{nfvExperimentPath}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatioIndex}/locality={locality}/" \
                 f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategy}/doMVCC={doMVCCIndex}/udfComplexity={udfComplexity}/" \
                 "latency.csv"
            
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['latency'])
                data[readRatioIndex][doMVCCIndex] = [value / 1e6 for value in df['latency'].tolist()]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                data[readRatioIndex][doMVCCIndex] = []

    fig, ax = plt.subplots(figsize=(7, 4))
    
    boxplot_data = []
    boxplot_labels = []  # This will hold unique keySkew values
    colors = ['#0060bf', '#8c0b0b']  # Different colors for different ccStrategies
    displayedStrategyList = ["SVCC", "MVCC"]
    
    positions = []  # Will store x-axis positions for the box plots
    num_cc_strategies = len(doMVCCList)
    width_per_group = 0.8  # Space allocated per keySkew group

    for i, readRatioIndex in enumerate(readRatioList):
        for j, doMVCCIndex in enumerate(doMVCCList):
            latency_values = data[readRatioIndex][doMVCCIndex]
            if latency_values:  # If there's data for this combination
                boxplot_data.append(latency_values)
                positions.append(i * (num_cc_strategies + 1) + j)
        
        boxplot_labels.append(f'{readRatioIndex}')

    bplot = ax.boxplot(boxplot_data, positions=positions, patch_artist=True, widths=0.6)
    for patch, color in zip(bplot['boxes'], colors * len(readRatioList)):
        patch.set_facecolor(color)

    for median in bplot['medians']:
        median.set(linewidth=2.5) # Set the median line width

    ax.set_xticks([i * (num_cc_strategies + 1) + num_cc_strategies / 2 - 0.5 for i in range(len(readRatioList))])
    ax.set_xticklabels(boxplot_labels, fontsize=16)
    ax.tick_params(axis='y', labelsize=16)
    
    ax.set_ylabel('Latency (s)', fontsize=18)
    ax.set_xlabel('Read Ratio', fontsize=18)

    handles = [plt.Line2D([0], [0], color=color, lw=10) for color in colors]
    ax.legend(handles=handles, labels=displayedStrategyList, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=2, fontsize=17)
    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)

    figure_name_png = f'5.4.2_readRatio_lat.png'
    figure_dir = os.path.join(nfvExperimentPath, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name_png))


# Basic params
app = "nfv_test"
expID = "5.4.2"
vnfID = 11
numItems = 5000
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
udfComplexity = 5
gcCheckInterval = 1000000
gcBatchInterval = 1000000

readRatioList = [0, 25, 50, 75, 100]
doMVCCList = [0, 1]



def run_SAC_ReadRatio_tradeoff(root_dir, exp_dir):
    shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
    print(f"Shell script path: {shellScriptPath}")
    generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy,
                         doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio, shellScriptPath,
                         gcCheckInterval, gcBatchInterval, root_dir)

    execute_bash_script(shellScriptPath)

def plot_readRatio_throughput(exp_dir):
    plot_keyskew_throughput_figure(exp_dir, expID, vnfID, numPackets, numItems, numInstances,
                                   numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity,
                                   keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy, 
                                   readRatioList, doMVCCList)

def plot_readRatio_latency(exp_dir):
    plot_keyskew_latency_boxplot(exp_dir,
                                 expID, vnfID, numPackets, numItems, numInstances, numTPGThreads, numOffloadThreads, 
                                 puncInterval, doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, 
                                 scopeRatio, ccStrategy,
                                 readRatioList, doMVCCList)

    print("Done")

def main(root_dir, exp_dir):

    print(f"Root directory: {root_dir}")
    print(f"Experiment directory: {exp_dir}")

    run_SAC_ReadRatio_tradeoff(root_dir, exp_dir)
    plot_readRatio_throughput(exp_dir)
    plot_readRatio_latency(exp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)