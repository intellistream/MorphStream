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
  for workloadSkew in 0 25 50 75 100
  do
    for ccStrategy in Offloading Proactive
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


def plot_throughput_figure(nfvExperimentPath,
                        expID, vnfID, numPackets, numItems, numInstances, 
                        numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity, 
                        keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy,
                        workloadSkewList, ccStrategyList):
    
    colors = ['white', 'white']
    hatches = ['\\\\\\', '////']
    hatch_colors = ['#d97400', '#0060bf']

    # Prepare the structure to hold data
    data = {workloadSkewIndex: {} for workloadSkewIndex in workloadSkewList}

    # Iterate over the patterns and ccStrategies
    for workloadSkewIndex in workloadSkewList:
        for ccStrategyIndex in ccStrategyList:
            outputFilePath = f"{nfvExperimentPath}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkewIndex}/readRatio={readRatio}/locality={locality}/" \
                 f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategyIndex}/doMVCC={doMVCC}/udfComplexity={udfComplexity}/" \
                 "throughput.csv"

            # Read the CSV file
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                data[workloadSkewIndex][ccStrategyIndex] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                data[workloadSkewIndex][ccStrategyIndex] = None

    # Convert the data into a NumPy array and normalize by 10^6
    throughput_data = np.array([[data[workloadSkewIndex][ccStrategyIndex] if data[workloadSkewIndex][ccStrategyIndex] is not None else 0
                                 for ccStrategyIndex in ccStrategyList] for workloadSkewIndex in workloadSkewList]) / 1e6

    # Plotting parameters
    bar_width = 0.2
    index = np.arange(len(workloadSkewList))

    # Plot the data
    fig, ax = plt.subplots(figsize=(7, 5))

    displayedStrategyList = ["Passive Resolution", "Proactive Resolution"]
    for i, strategy in enumerate(ccStrategyList):
        ax.bar(index + i * bar_width, throughput_data[:, i], color=colors[i], hatch=hatches[i],
               edgecolor=hatch_colors[i], width=bar_width, label=displayedStrategyList[i])

    # Set x-axis labels and positions
    ax.set_xticks([r + bar_width for r in range(len(workloadSkewList))])
    ax.set_xticklabels(workloadSkewList, fontsize=16)
    ax.set_ylabel('Throughput (M req/sec)', fontsize=18, labelpad=12)
    ax.set_xlabel('Trojan Detector Workload Variations', fontsize=18, labelpad=12)

    ax.tick_params(axis='y', labelsize=14)

    # Set labels and title
    ax.set_xlabel('Workload Skewness', fontsize=18)
    ax.set_ylabel('Throughput (Million req/sec)', fontsize=18)

    # Create custom legend with hatches
    handles = [Patch(facecolor=color, edgecolor=hatchcolor, hatch=hatch, label=label)
               for color, hatchcolor, hatch, label in zip(colors, hatch_colors, hatches, displayedStrategyList)]
    ax.legend(handles=handles, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=2, fontsize=16)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    # Save the figure in the same directory as the script
    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.4.1_workloadSkew_complexity={udfComplexity}.pdf'
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
    expID = "5.4.1"
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
    ccStrategy = "Partitioning"
    doMVCC = 0
    udfComplexity = 0

    rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    indicatorPath = f"{rootDir}/indicators/{expID}.txt"
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID

    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, 
                         doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio, shellScriptPath)
    
    execute_bash_script(shellScriptPath)

    workloadSkewList = [0, 25, 50, 75, 100]
    ccStrategyList = ["Offloading", "Proactive"]

    plot_throughput_figure(rootDir, expID, vnfID, numPackets, numItems, numInstances,
                                   numTPGThreads, numOffloadThreads, puncInterval, doMVCC, udfComplexity,
                                   keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy, 
                                   workloadSkewList, ccStrategyList)

