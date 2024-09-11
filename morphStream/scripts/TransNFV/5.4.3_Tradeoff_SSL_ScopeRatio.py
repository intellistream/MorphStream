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
  for scopeRatio in 0 25 50 75 100
  do
    for ccStrategy in Partitioning Replication Offloading
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
                        scopeRatioList, ccStrategyList):
    
    colors = ['white', 'white', 'white']
    hatches = ['\\\\\\', '////', '--']
    hatch_colors = ['#8c0b0b', '#0060bf', '#d97400']

    # Prepare the structure to hold data
    data = {readRatioIndex: {} for readRatioIndex in scopeRatioList}

    # Iterate over the patterns and ccStrategies
    for scopeRatioIndex in scopeRatioList:
        for ccStrategyIndex in ccStrategyList:
            outputFilePath = f"{nfvExperimentPath}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/" \
                 f"scopeRatio={scopeRatioIndex}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategyIndex}/doMVCC={doMVCC}/udfComplexity={udfComplexity}/" \
                 "throughput.csv"

            # Read the CSV file
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                data[scopeRatioIndex][ccStrategyIndex] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                data[scopeRatioIndex][ccStrategyIndex] = None

    # print(data)
    # Convert the data into a NumPy array and normalize by 10^6
    throughput_data = np.array([[data[localityIndex][ccStrategyIndex] if data[localityIndex][ccStrategyIndex] is not None else 0
                                 for ccStrategyIndex in ccStrategyList] for localityIndex in scopeRatioList]) / 1e6

    # Plotting parameters
    bar_width = 0.2
    index = np.arange(len(scopeRatioList))

    # Plot the data
    fig, ax = plt.subplots(figsize=(7, 5))

    displayedStrategyList = ["Partitioned", "Replicated", "Global"]
    for i, strategy in enumerate(ccStrategyList):
        ax.bar(index + i * bar_width, throughput_data[:, i], color=colors[i], hatch=hatches[i],
               edgecolor=hatch_colors[i], width=bar_width, label=displayedStrategyList[i])

    # Set x-axis labels and positions
    ax.set_xticks([r + bar_width for r in range(len(scopeRatioList))])
    ax.set_xticklabels(scopeRatioList, fontsize=16)
    ax.set_ylabel('Throughput (M req/sec)', fontsize=18, labelpad=12)
    ax.set_xlabel('Trojan Detector Workload Variations', fontsize=18, labelpad=12)
    ax.tick_params(axis='y', labelsize=14)
    ax.set_xlabel('Scope Ratio', fontsize=18)
    ax.set_ylabel('Throughput (Million req/sec)', fontsize=18)
    handles = [Patch(facecolor=color, edgecolor=hatchcolor, hatch=hatch, label=label)
               for color, hatchcolor, hatch, label in zip(colors, hatch_colors, hatches, displayedStrategyList)]
    ax.legend(handles=handles, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=3, fontsize=16)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    # Save the figure in the same directory as the script
    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'{expID}_scopeRatio_range={numItems}_complexity={udfComplexity}.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))  # Save the figure
    plt.savefig(os.path.join(figure_dir, figure_name))  # Save the figure

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))  # Save the figure


if __name__ == "__main__":
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

    rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    indicatorPath = f"{rootDir}/indicators/{expID}.txt"
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID

    # generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
    #                      numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, 
    #                      doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio, shellScriptPath)
    
    # execute_bash_script(shellScriptPath)

    scopeRatioList = [0, 25, 50, 75, 100]
    ccStrategyList = ["Partitioning", "Replication", "Offloading"]

    plot_keyskew_throughput_figure(rootDir, expID, vnfID, numPackets, numItems, numInstances,
                                   numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity,
                                   keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy, 
                                   scopeRatioList, ccStrategyList)

