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
import itertools



def generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy,
                         doMVCC, udfComplexity,
                         keySkew, workloadSkew, readRatio, locality, scopeRatio,
                         workloadInterval, monitorWindowSize, hardcodeSwitch,
                         script_path, root_dir):

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
  monitorWindowSize={monitorWindowSize}
  workloadInterval={workloadInterval}
  hardcodeSwitch={hardcodeSwitch}
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
          --monitorWindowSize $monitorWindowSize \\
          --workloadInterval $workloadInterval \\
          --hardcodeSwitch $hardcodeSwitch
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
    --monitorWindowSize $monitorWindowSize \\
    --workloadInterval $workloadInterval \\
    --hardcodeSwitch $hardcodeSwitch
}}

function Per_Phase_Experiment() {{
    ResetParameters
    for ccStrategy in Adaptive OpenNF CHC S6
    do
      runTStream
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



def get_breakdown_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy):
    return f"{exp_dir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                  f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
                  f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
                  f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
                  f"doMVCC={doMVCC}/udfComplexity={udfComplexity}/workloadInterval={workloadInterval}/monitorWindowSize={monitorWindowSize}/hardcodeSwitch={0}/breakdown.csv"

def get_footprint_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy):
    return f"{exp_dir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                  f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
                  f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
                  f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
                  f"doMVCC={doMVCC}/udfComplexity={udfComplexity}/footprint.csv"

def read_breakdown_from_file(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            double_values = tuple(map(float, row))
            return (*double_values, None)

def read_footprint_data(csv_file_path):
    timestamps = []
    footprints = []
    with open(csv_file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            try:
                timestamps.append(float(row[0]))
                footprints.append(float(row[1]))
            except ValueError:
                print(f"Skipping invalid value: {row[0]}")
    return footprints


def draw_breakdown_comparison_plot(exp_dir):
    bar_width = 0.5
    systemList = ['OpenNF', 'CHC', 'S6', 'Adaptive']
    breakdownList = ['Parsing', 'Sync', 'Useful', 'Switching']
    colors = ['white', 'white', 'white', 'white']
    hatches = ['\\\\\\', '////', '---', 'xxx']
    hatch_colors = ['#8c0b0b', '#0060bf', '#d97400', '#b313f2']
    system_breakdown_data = {system: {breakdown: 0.0 for breakdown in breakdownList} for system in systemList}

    for system in systemList:
        outputFilePath = get_breakdown_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, system)
        breakdown_values = read_breakdown_from_file(outputFilePath)
        perPhaseParsingTime = breakdown_values[0] if (breakdown_values and breakdown_values[0] > 0) else 0
        perPhaseSyncTime = breakdown_values[1] if (breakdown_values and breakdown_values[1] > 0) else 0
        perPhaseUsefulTime = breakdown_values[2] if (breakdown_values and breakdown_values[2] > 0) else 0
        perPhaseSwitchingTime = breakdown_values[3] if (breakdown_values and breakdown_values[3] > 0) else 0

        system_breakdown_data[system]['Parsing'] += perPhaseParsingTime
        system_breakdown_data[system]['Sync'] += perPhaseSyncTime
        system_breakdown_data[system]['Useful'] += perPhaseUsefulTime
        system_breakdown_data[system]['Switching'] += perPhaseSwitchingTime

    parsing_times = [system_breakdown_data[system]['Parsing'] for system in systemList]
    sync_times = [system_breakdown_data[system]['Sync'] for system in systemList]
    useful_times = [system_breakdown_data[system]['Useful'] for system in systemList]
    switching_times = [system_breakdown_data[system]['Switching'] for system in systemList]

    fig, ax = plt.subplots(figsize=(7, 4.5))

    indices = np.arange(len(systemList))

    p1 = ax.barh(indices, parsing_times, height=bar_width, label='Parsing', hatch='\\\\\\', color=colors[0], edgecolor=hatch_colors[0])
    p2 = ax.barh(indices, sync_times, height=bar_width, label='Sync', hatch='////', color=colors[1], edgecolor=hatch_colors[1],
                 left=parsing_times)
    p3 = ax.barh(indices, useful_times, height=bar_width, label='State Access', hatch='---', color=colors[2], edgecolor=hatch_colors[2],
                 left=np.add(parsing_times, sync_times))
    p4 = ax.barh(indices, switching_times, height=bar_width, label='Switching', hatch='xxx', color=colors[3], edgecolor=hatch_colors[3],
                 left=np.add(np.add(parsing_times, sync_times), useful_times))

    ax.set_yticks(indices)
    ax.set_yticklabels(systemList, fontsize=15)
    plt.xticks(fontsize=15)
    ax.set_xlabel('Execution Time (ms)', fontsize=18)
    ax.legend(bbox_to_anchor=(0.42, 1.2), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)

    plt.tight_layout()
    plt.subplots_adjust(left=0.15, right=0.98, top=0.85, bottom=0.15)

    figure_name = f'5.3.1_breakdown.pdf'
    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))



vnfID = 11
numItems = 1000
numPackets = 1400000
numInstances = 4
app = "nfv_test"
# workloadIntervalList  = [10000, 20000, 50000, 100000, 200000]
# monitorWindowSizeList = [10000, 20000, 50000, 100000]
workloadIntervalList = [20000, 40000, 100000, 200000]
monitorWindowSizeList = [20000, 40000, 100000, 200000]

workloadInterval = 40000
monitorWindowSize = 40000

# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
ccStrategy = "Adaptive"
doMVCC = 0
udfComplexity = 1

# Workload chars
expID = "5.3"
keySkew = 0
workloadSkew = 0
readRatio = 0
locality = 0
scopeRatio = 0


def run_breakdown_analysis(root_dir, exp_dir):
    shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
    print(f"Shell script path: {shellScriptPath}")
    generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity,
                         keySkew, workloadSkew, readRatio, locality, scopeRatio,
                         workloadInterval, monitorWindowSize, 0,
                         shellScriptPath, root_dir)

    execute_bash_script(shellScriptPath)


def main(root_dir, exp_dir):

    print(f"Root directory: {root_dir}")
    print(f"Experiment directory: {exp_dir}")

    run_breakdown_analysis(root_dir, exp_dir)
    draw_breakdown_comparison_plot(exp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)
    print("Preliminary study results generated")