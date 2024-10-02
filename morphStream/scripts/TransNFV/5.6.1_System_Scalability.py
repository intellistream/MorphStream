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
                         doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio, script_path):
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
          --scopeRatio $scopeRatio
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
    --scopeRatio $scopeRatio
}}

function iterateExperiments() {{
  ResetParameters
  for numThread in 1 2 4 8 16
  do
    numOffloadThreads=$numThread
    numInstances=$numThread
    for ccStrategy in Offloading OpenNF S6 CHC
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
                        numTPGThreads, numOffloadThreads, puncInterval, 
                        doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio,
                        numThreadsList, ccStrategyList):
    
    markers = ['o', 's', 'D', '^']  # Define different markers for each strategy
    colors = ['#0060bf', '#8c0b0b', '#d97400', '#7812a1']  # Use the hatch colors for the lines
    linestyles = ['-', '--', '-.', ':']  # Different line styles for each strategy

    # Prepare the structure to hold data
    data = {keySkew: {} for keySkew in numThreadsList}

    # Iterate over the patterns and ccStrategies
    for numThreadsIndex in numThreadsList:
        for ccStrategyIndex in ccStrategyList:
            outputFilePath = f"{nfvExperimentPath}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numThreadsIndex}/" \
                 f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/" \
                 f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numThreadsIndex}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategyIndex}/doMVCC={doMVCC}/udfComplexity={udfComplexity}/" \
                 "throughput.csv"

            # Read the CSV file
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                data[numThreadsIndex][ccStrategyIndex] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                data[numThreadsIndex][ccStrategyIndex] = None

    # Convert the data into a NumPy array and normalize by 10^6
    throughput_data = np.array([[data[numThreads][ccStrategy] if data[numThreads][ccStrategy] is not None else 0
                                 for ccStrategy in ccStrategyList] for numThreads in numThreadsList]) / 1e6
    
    print(throughput_data)

    # Plot the data as a line chart
    fig, ax = plt.subplots(figsize=(7, 4.5))

    displayedStrategyList = ["TransNFV", "OpenNF", "S6", "CHC"]
    indices = range(len(numThreadsList))  # Use indices for evenly spaced x-axis

    for i, strategy in enumerate(ccStrategyList):
        ax.plot(indices, throughput_data[:, i], marker=markers[i], color=colors[i], linestyle=linestyles[i], 
                label=displayedStrategyList[i], markersize=8, linewidth=2)

    ax.set_xticks(indices)  # Set the ticks to the indices
    ax.set_xticklabels(numThreadsList, fontsize=16)  # Set the tick labels to the actual number of threads
    ax.tick_params(axis='y', labelsize=14)
    ax.set_xlabel('Number of Parallel Executors', fontsize=18)
    ax.set_ylabel('Throughput (Million req/sec)', fontsize=18)

    plt.legend(bbox_to_anchor=(0.45, 1.23), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    plt.tight_layout()
    # plt.subplots_adjust(left=0.12, right=0.98, top=0.97, bottom=0.15)
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.6.1_scalability_range={numItems}_complexity={udfComplexity}.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))  # Save the figure

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))  # Save the figure





if __name__ == "__main__":
    # Basic params
    app = "nfv_test"
    expID = "5.6.1"
    vnfID = 11
    numItems = 10000
    numPackets = 400000

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
    ccStrategy = "Partitioning"
    doMVCC = 0
    udfComplexity = 10
    numInstances = 4
    numThreadsList = [1, 2, 4, 8, 16]
    ccStrategyList = ["Offloading", "OpenNF", "S6", "CHC"]

    rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID

    # generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
    #                      numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, 
    #                      doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio, shellScriptPath)
    
    # execute_bash_script(shellScriptPath)

    plot_keyskew_throughput_figure(rootDir, expID, vnfID, numPackets, numItems, numInstances,
                                   numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity,
                                   keySkew, workloadSkew, readRatio, locality, scopeRatio, numThreadsList, ccStrategyList)

