import subprocess
import os
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import argparse


def generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval,
                         doMVCC, udfComplexity, keySkew, workloadSkew, readRatio, locality, scopeRatio, shell_script_path, root_dir):
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
    for ccStrategy in Partitioning Replication Offloading
    do
        runTStream
    done
}}

iterateExperiments
ResetParameters
"""

    with open(shell_script_path, "w") as file:
        file.write(script_content)
    os.chmod(shell_script_path, 0o755)


def stream_reader(pipe, pipe_name):
    with pipe:
        for line in iter(pipe.readline, ''):
            print(f"{pipe_name}: {line.strip()}")


def execute_bash_script(shell_script_path):
    print(f"Executing bash script: {shell_script_path}")
    process = subprocess.Popen(["bash", shell_script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
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


def plot_throughput_figure(exp_dir):
    colors = ['white', 'white', 'white']
    hatches = ['\\\\\\', '////', 'xxx']
    hatch_colors = ['#8c0b0b', '#0060bf', '#129c03']
    data = {readRatioIndex: {} for readRatioIndex in range(4)}

    for workloadIndex in range(4):
        keySkew, workloadSkew, readRatio, localityIndex, scopeRatio = workloadList[workloadIndex]
        for ccStrategyIndex in strategyList:
            outputFilePath = f"{exp_dir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                             f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={localityIndex}/" \
                             f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                             f"puncInterval={puncInterval}/ccStrategy={ccStrategyIndex}/doMVCC={doMVCC}/udfComplexity={udfComplexity}/" \
                             "throughput.csv"
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                data[workloadIndex][ccStrategyIndex] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                data[workloadIndex][ccStrategyIndex] = None

    throughput_data = np.array(
        [[data[workloadIndex][ccStrategyIndex] if data[workloadIndex][ccStrategyIndex] is not None else 0
          for ccStrategyIndex in strategyList] for workloadIndex in range(4)]) / 1e6

    bar_width = 0.2
    index = np.arange(len(range(4)))
    fig, ax = plt.subplots(figsize=(9, 3))

    displayedStrategyList = ["State\nPartitioning", "State\nReplication", "Operation\nOffloading"]
    for i, strategy in enumerate(strategyList):
        ax.bar(index + i * bar_width, throughput_data[:, i], color=colors[i], hatch=hatches[i],
               edgecolor=hatch_colors[i], width=bar_width, label=displayedStrategyList[i])

    ax.set_xticks([r + bar_width for r in range(len(range(4)))])
    ax.set_xticklabels([f"Phase {i}" for i in range(4)], fontsize=14)
    ax.tick_params(axis='y', labelsize=14)
    ax.set_xlabel('Workload with Variations', fontsize=18)
    ax.set_ylabel('Throughput\n(Million req/sec)', fontsize=18, labelpad=15)
    handles = [Patch(facecolor=color, edgecolor=hatchcolor, hatch=hatch, label=label)
               for color, hatchcolor, hatch, label in zip(colors, hatch_colors, hatches, displayedStrategyList)]
    ax.legend(handles=handles, bbox_to_anchor=(1.4, 1), loc='upper right', ncol=1, fontsize=15, columnspacing=0.5, labelspacing=1.2)

    plt.tight_layout()
    plt.subplots_adjust(left=0.15, right=0.75, top=0.95, bottom=0.2)

    figure_name = f'{expID}_preStudy.pdf'
    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))  # Save the figure
    plt.savefig(os.path.join(figure_dir, figure_name))  # Save the figure


 # Basic params
app = "nfv_test"
expID = "5.1"
vnfID = 11
numItems = 1000
numPackets = 400000
numInstances = 4

# Workload chars
workload1 = [0, 0, 0, 100, 0]
workload2 = [0, 0, 90, 0, 0]
workload3 = [0, 0, 0, 0, 0]
workload4 = [150, 0, 0, 0, 0]

workloadList = [
    workload1, workload2, workload3, workload4
]

# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
doMVCC = 0
udfComplexity = 0
strategyList = ["Partitioning", "Replication", "Offloading"]


def run_pre_study(root_dir, exp_dir):
    shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
    print(f"Shell script path: {shellScriptPath}")

    for workload in workloadList:
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload
        generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                             numTPGThreads, numOffloadThreads, puncInterval,
                             doMVCC, udfComplexity,
                             keySkew, workloadSkew, readRatio, locality, scopeRatio,
                             shellScriptPath, root_dir)
        execute_bash_script(shellScriptPath)


def main(root_dir, exp_dir):

    print(f"Root directory: {root_dir}")
    print(f"Experiment directory: {exp_dir}")

    run_pre_study(root_dir, exp_dir)
    plot_throughput_figure(exp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)