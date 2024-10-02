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


def generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, 
                         doMVCC, udfComplexity, 
                         keySkew, workloadSkew, readRatio, locality, scopeRatio, 
                         monitorWindowSize, workloadInterval,
                         script_path):
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
  monitorWindowSize={monitorWindowSize}
  workloadInterval={workloadInterval}
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
          --monitorWindowSize $monitorWindowSize \\
          --workloadInterval $workloadInterval
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
    --monitorWindowSize $monitorWindowSize \\
    --workloadInterval $workloadInterval
}}

function Per_Phase_Experiment() {{
    ResetParameters
    for workloadInterval in 10000 20000 50000 100000 200000
    do
        for monitorWindowSize in 1000 2000 5000 10000
        do
            monitorWindowSize = $monitorWindowSize
            workloadInterval = $workloadInterval
            runTStream
        done
    done
}}

Per_Phase_Experiment

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


def get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy, workloadInterval, monitorWindowSize):
    return f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                  f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
                  f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
                  f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
                  f"doMVCC={doMVCC}/udfComplexity={udfComplexity}/workloadInterval={workloadInterval}/monitorWindowSize={monitorWindowSize}/throughput.csv"

def get_throughput_from_file(outputFilePath):
    try:
        df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
        return df['Throughput'].iloc[0]
    except Exception as e:
        print(f"Failed to read {outputFilePath}: {e}")
        return None







def plot_keyskew_throughput_figure():
    
    markers = ['o', 's', 'D', '^']  # Define different markers for each strategy
    colors = ['#0060bf', '#8c0b0b', '#d97400', '#7812a1']  # Use the hatch colors for the lines
    linestyles = ['-', '--', '-.', ':']  # Different line styles for each strategy

    # Prepare the structure to hold data
    data = {workloadInterval: {} for workloadInterval in workloadIntervalList}

    # Iterate over the patterns and ccStrategies
    for workloadInterval in workloadIntervalList:
        for monitorWindowSize in monitorWindowSizeList:
            outputFilePath = f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/" \
                 f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategy}/doMVCC={doMVCC}/udfComplexity={udfComplexity}/" \
                 f"workloadInterval={workloadInterval}/monitorWindowSize={monitorWindowSize}/" \
                 "throughput.csv"

            # Read the CSV file
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                data[workloadInterval][monitorWindowSize] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                data[workloadInterval][monitorWindowSize] = None

    # Convert the data into a NumPy array and normalize by 10^6
    throughput_data = np.array([[data[numThreads][ccStrategy] if data[numThreads][ccStrategy] is not None else 0
                                 for ccStrategy in monitorWindowSizeList] for numThreads in workloadIntervalList]) / 1e6
    
    print(throughput_data)

    # Plot the data as a line chart
    fig, ax = plt.subplots(figsize=(7, 4.5))

    indices = range(len(workloadIntervalList))  # Use indices for evenly spaced x-axis

    for i, strategy in enumerate(monitorWindowSizeList):
        ax.plot(indices, throughput_data[:, i], marker=markers[i], color=colors[i], linestyle=linestyles[i], 
                label=f"{monitorWindowSizeList[i]//1000} K", markersize=8, linewidth=2)

    ax.set_xticks(indices)  # Set the ticks to the indices
    ax.set_xticklabels([x // 1000 for x in workloadIntervalList], fontsize=16)  # Divide x-axis values by 1000
    ax.tick_params(axis='y', labelsize=14)
    ax.set_xlabel('Workload Interval Length (K Packets)', fontsize=18)
    ax.set_ylabel('Throughput (Million req/sec)', fontsize=18)

    plt.legend(bbox_to_anchor=(0.45, 1.23), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    plt.tight_layout()
    # plt.subplots_adjust(left=0.12, right=0.98, top=0.97, bottom=0.15)
    plt.subplots_adjust(left=0.15, right=0.98, top=0.85, bottom=0.15)

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.6.2_windowSize_range={numItems}_complexity={udfComplexity}.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))  # Save the figure

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))  # Save the figure
    

vnfID = 11
numItems = 10000
numPackets = 400000
numInstances = 4
app = "nfv_test"
workloadIntervalList  = [10000, 20000, 50000, 100000, 200000]
monitorWindowSizeList = [1000, 2000, 5000, 10000]

# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
ccStrategy = "Adaptive"
doMVCC = 0
udfComplexity = 10
rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"

# Workload chars
expID = "5.6.2"
keySkew = 0
workloadSkew = 0
readRatio = 0
locality = 0
scopeRatio = 0
monitorWindowSize = 1000
workloadInterval = 10000


def exp():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID
    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         keySkew, workloadSkew, readRatio, locality, scopeRatio, monitorWindowSize, workloadInterval,
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)




if __name__ == "__main__":
    # exp()

    plot_keyskew_throughput_figure()
    print("Done")