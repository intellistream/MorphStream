import subprocess
import os
import time
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import csv


def generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, 
                         doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio, script_path,
                         gcCheckInterval, gcBatchInterval):
    script_content = f"""#!/bin/bash

function ResetParameters() {{
  app="{app}"
  expID="{expID}"
  vnfID="{vnfID}"
  nfvExperimentPath="{rootDir}"
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
  echo "java -Xms100g -Xmx100g -Xss10M -jar /home/zhonghao/IdeaProjects/transNFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
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
  java -Xms100g -Xmx100g -Xss10M -jar /home/zhonghao/IdeaProjects/transNFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
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
  for keySkew in 0 50 100 150 200 250
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


def plot_keyskew_throughput_figure(nfvExperimentPath,
                        expID, vnfID, numPackets, numItems, numInstances, 
                        numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity, 
                        keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy,
                        keySkewList, sacList):
    
    colors = ['white', 'white']
    hatches = ['\\\\\\', '////']
    hatch_colors = ['#0060bf', '#8c0b0b']
    data = {keySkewIndex: {} for keySkewIndex in keySkewList}

    for keySkewIndex in keySkewList:
        for sacOption in sacList:
            outputFilePath = f"{nfvExperimentPath}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkewIndex}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/" \
                 f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategy}/doMVCC={sacOption}/udfComplexity={udfComplexity}/" \
                 "throughput.csv"

            # Read the CSV file
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                data[keySkewIndex][sacOption] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                data[keySkewIndex][sacOption] = None

    throughput_data = np.array([[data[keySkew][sacOption] if data[keySkew][sacOption] is not None else 0
                                 for sacOption in sacList] for keySkew in keySkewList]) / 1e6

    bar_width = 0.2
    index = np.arange(len(keySkewList))
    fig, ax = plt.subplots(figsize=(7, 5))

    displayedStrategyList = ["SVCC", "MVCC"]
    for i, strategy in enumerate(sacList):
        ax.bar(index + i * bar_width, throughput_data[:, i], color=colors[i], hatch=hatches[i],
               edgecolor=hatch_colors[i], width=bar_width, label=displayedStrategyList[i])

    ax.set_xticks([r + bar_width for r in range(len(keySkewList))])
    ax.set_xticklabels(keySkewList, fontsize=16)
    ax.tick_params(axis='y', labelsize=14)

    ax.set_xlabel('Key Skewness', fontsize=18)
    ax.set_ylabel('Throughput (Million req/sec)', fontsize=18)
    handles = [Patch(facecolor=color, edgecolor=hatchcolor, hatch=hatch, label=label)
               for color, hatchcolor, hatch, label in zip(colors, hatch_colors, hatches, displayedStrategyList)]
    ax.legend(handles=handles, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=2, fontsize=16)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'{expID}_key_range{numItems}_comp{udfComplexity}_read{readRatio}.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))  # Save the figure
    plt.savefig(os.path.join(figure_dir, figure_name))  # Save the figure

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))  # Save the figure


def plot_keyskew_latency_boxplot(nfvExperimentPath,
                                 expID, vnfID, numPackets, numItems, numInstances, numTPGThreads, numOffloadThreads, 
                                 puncInterval, doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, 
                                 scopeRatio, ccStrategy,
                                 keySkewList, doMVCCList):
    
    data = {keySkew: {doMVCCIndex: [] for doMVCCIndex in doMVCCList} for keySkew in keySkewList}
    
    for keySkewIndex in keySkewList:
        for doMVCCIndex in doMVCCList:
            outputFilePath = f"{nfvExperimentPath}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkewIndex}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/" \
                 f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategy}/doMVCC={doMVCCIndex}/udfComplexity={udfComplexity}/" \
                 "latency.csv"
            
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['latency'])
                data[keySkewIndex][doMVCCIndex] = [value / 1e6 for value in df['latency'].tolist()]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                data[keySkewIndex][doMVCCIndex] = []

    fig, ax = plt.subplots(figsize=(7, 4.5))
    
    boxplot_data = []
    boxplot_labels = []  # This will hold unique keySkew values
    colors = ['#0060bf', '#8c0b0b']  # Different colors for different ccStrategies
    displayedStrategyList = ["SVCC", "MVCC"]
    
    positions = []  # Will store x-axis positions for the box plots
    num_cc_strategies = len(doMVCCList)
    width_per_group = 0.8  # Space allocated per keySkew group

    for i, keySkewIndex in enumerate(keySkewList):
        for j, doMVCCIndex in enumerate(doMVCCList):
            latency_values = data[keySkewIndex][doMVCCIndex]
            if latency_values:  # If there's data for this combination
                boxplot_data.append(latency_values)
                positions.append(i * (num_cc_strategies + 1) + j)
        
        boxplot_labels.append(f'{keySkewIndex}')

    bplot = ax.boxplot(boxplot_data, positions=positions, patch_artist=True, widths=0.6)
    for patch, color in zip(bplot['boxes'], colors * len(keySkewList)):
        patch.set_facecolor(color)

    for median in bplot['medians']:
        median.set(linewidth=2.5) # Set the median line width

    ax.set_xticks([i * (num_cc_strategies + 1) + num_cc_strategies / 2 - 0.5 for i in range(len(keySkewList))])
    ax.set_xticklabels(boxplot_labels, fontsize=16)
    ax.tick_params(axis='y', labelsize=16)
    
    ax.set_ylabel('Latency (s)', fontsize=18)
    ax.set_xlabel('Key Skewness', fontsize=18)

    handles = [plt.Line2D([0], [0], color=color, lw=10) for color in colors]
    ax.legend(handles=handles, labels=displayedStrategyList, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=2, fontsize=17)
    plt.tight_layout()

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.4.2_key_range{numItems}_comp{udfComplexity}_read{readRatio}_lat.pdf'
    figure_name_png = f'5.4.2_key_range{numItems}_comp{udfComplexity}_lat.png'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    # plt.savefig(os.path.join(figure_dir, figure_name))
    plt.savefig(os.path.join(figure_dir, figure_name_png))

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    # plt.savefig(os.path.join(local_figure_dir, figure_name))
    plt.savefig(os.path.join(local_figure_dir, figure_name_png))


if __name__ == "__main__":
    # Basic params
    app = "nfv_test"
    expID = "5.4.2"
    vnfID = 11
    numItems = 5000
    numPackets = 400000
    numInstances = 4

    # Workload chars
    keySkew = 75
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

    rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    indicatorPath = f"{rootDir}/indicators/{expID}.txt"
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID

    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, 
                         doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio, shellScriptPath,
                         gcCheckInterval, gcBatchInterval)
    
    execute_bash_script(shellScriptPath)
    
    keySkewList = [0, 50, 100, 150, 200, 250]
    sacList = [0, 1]

    plot_keyskew_throughput_figure(rootDir, expID, vnfID, numPackets, numItems, numInstances,
                                   numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity,
                                   keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy, 
                                   keySkewList, sacList)
    
    plot_keyskew_latency_boxplot(rootDir, expID, vnfID, numPackets, numItems, numInstances, 
                                 numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity, 
                                 keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy,
                                 keySkewList, sacList)
