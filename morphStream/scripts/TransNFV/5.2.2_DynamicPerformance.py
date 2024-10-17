import argparse
import subprocess
import os
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import csv


def generate_bash_script(app, exp_dir, vnfID, rootDir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy,
                         doMVCC, udfComplexity,
                         keySkew, workloadSkew, readRatio, locality, scopeRatio,
                         shell_script_path, root_dir):
    script_content = f"""#!/bin/bash

function ResetParameters() {{
  app="{app}"
  expID="{exp_dir}"
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

function Per_Phase_Experiment() {{
    ResetParameters
    for ccStrategy in Partitioning Replication Offloading Proactive OpenNF CHC S6
    do
      runTStream
    done
}}

Per_Phase_Experiment

"""

    with open(shell_script_path, "w") as file:
        file.write(script_content)
    os.chmod(shell_script_path, 0o755)

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


def get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy):
    return f"{exp_dir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                  f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
                  f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
                  f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
                  f"doMVCC={doMVCC}/udfComplexity={udfComplexity}/throughput.csv"

def get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy):
    return f"{exp_dir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                  f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
                  f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
                  f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
                  f"doMVCC={doMVCC}/udfComplexity={udfComplexity}/latency.csv"

def get_throughput_from_file(outputFilePath):
    try:
        df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
        return df['Throughput'].iloc[0]
    except Exception as e:
        print(f"Failed to read {outputFilePath}: {e}")
        return None

def draw_throughput_comparison_plot(exp_dir):
    systems = ['TransNFV', 'OpenNF', 'CHC', 'S6']
    colors = ['#AE48C8', '#F44040', '#15DB3F', '#E9AA18']
    markers = ['o', 's', '^', 'D']
    system_data = {system: [] for system in systems}

    for workload in workloadList:
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload
        
        transnfv_throughputs = []
        for ccStrategy in ["Partitioning", "Replication", "Offloading", "Proactive"]:
            outputFilePath = get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy)
            throughput = get_throughput_from_file(outputFilePath)
            if throughput:
                transnfv_throughputs.append(throughput)
        system_data['TransNFV'].append(max(transnfv_throughputs) / 1e6 if transnfv_throughputs else 0)

        outputFilePath_opennf = get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "OpenNF")
        system_data['OpenNF'].append(get_throughput_from_file(outputFilePath_opennf) / 1e6 or 0)

        outputFilePath_chc = get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "CHC")
        system_data['CHC'].append(get_throughput_from_file(outputFilePath_chc) / 1e6 or 0)

        outputFilePath_s6 = get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "S6")
        system_data['S6'].append(get_throughput_from_file(outputFilePath_s6) / 1e6 or 0)

    plt.figure(figsize=(7, 4.5))
    x_labels = [f"{i+1}" for i in range(len(workloadList))]
    x_positions = np.arange(len(workloadList))
    
    for i, system in enumerate(systems):
        plt.plot(x_positions, system_data[system], marker=markers[i], color=colors[i], label=system, markersize=8, 
         markeredgecolor='black', markeredgewidth=1.5)

    plt.xticks(x_positions, x_labels, fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel('Dynamic Workload Phases', fontsize=18)
    plt.ylabel('Throughput (Million req/sec)', fontsize=18)
    plt.legend(bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)
    ax = plt.gca()  
    yticks = ax.get_yticks()  # Automatically get y-axis tick positions
    ax.set_yticks(yticks)

    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)

    figure_name = f'5.2.2_dynamicWorkload.pdf'
    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))


def read_latencies(csv_file_path):
    latencies = []
    with open(csv_file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            try:
                latencies.append(float(row[0]) / 1000000)
            except ValueError:
                print(f"Skipping invalid value: {row[0]}")
    return latencies


def downsample_data(data, max_points=1000):
    if len(data) > max_points:
        indices = np.linspace(0, len(data) - 1, max_points, dtype=int)
        return data[indices]
    return data


def draw_latency_comparison_plot(exp_dir, max_points=1000):
    systems = ['TransNFV', 'OpenNF', 'CHC', 'S6']
    colors = ['#AE48C8', '#F44040', '#15DB3F', '#E9AA18']
    markers = ['o', 's', '^', 'D']
    system_latencies = {system: [] for system in systems}

    for workload in workloadList:
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload
        
        transnfv_latencies = []
        min_avg_latency = float('inf')
        min_latency_file = None

        for ccStrategy in ["Partitioning", "Replication", "Offloading", "Proactive"]:
            latency_file = get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy)
            latencies = read_latencies(latency_file)
            avg_latency = sum(latencies) / len(latencies) if latencies else float('inf')
            if avg_latency < min_avg_latency:
                min_avg_latency = avg_latency
                transnfv_latencies = latencies

        system_latencies['TransNFV'].extend(transnfv_latencies)

        latency_file_opennf = get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "OpenNF")
        system_latencies['OpenNF'].extend(read_latencies(latency_file_opennf))

        latency_file_chc = get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "CHC")
        system_latencies['CHC'].extend(read_latencies(latency_file_chc))

        latency_file_s6 = get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "S6")
        system_latencies['S6'].extend(read_latencies(latency_file_s6))

    plt.figure(figsize=(7, 4.5))

    for i, system in enumerate(systems):

        sorted_latencies = np.sort(system_latencies[system])
        downsampled_latencies = downsample_data(sorted_latencies, max_points)
        cdf = np.arange(1, len(downsampled_latencies) + 1) / len(downsampled_latencies)
        marker_positions = list(range(0, 1000, 100)) + [999]
        plt.plot(downsampled_latencies, cdf, marker=markers[i], color=colors[i], label=system, markersize=8,
                 markeredgecolor='black', markeredgewidth=1.5, markevery=marker_positions)

    plt.xticks(fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel('Latency (s)', fontsize=18)
    plt.ylabel('Cumulative Percent (%)', fontsize=18)
    # plt.legend(fontsize=16)
    plt.legend(bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)

    figure_name = f'5.2.2_dynamicWorkload_latency.png'
    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))

    

vnfID = 11
numItems = 10000
numPackets = 400000
numInstances = 4
app = "nfv_test"
expID = "5.2.2"

# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
ccStrategy = "Offloading"
doMVCC = 1
udfComplexity = 10
ccStrategyList = ["Partitioning", "Replication", "Offloading", "Proactive", "OpenNF", "CHC", "S6"]


# KeySkew, WorkloadSkew, ReadRatio, Locality, ScopeRatio

# Phase 1, decreasing locality
workload1 = [0, 0, 20, 100, 0]
workload2 = [0, 0, 20, 90, 0]
workload3 = [0, 0, 20, 80, 0]
workload4 = [0, 0, 20, 70, 0]

# Phase 2, increasing read ratio, increasing key skew
workload5 = [0, 0, 30, 0, 0]
workload6 = [0, 0, 40, 0, 0]
workload7 = [50, 0, 50, 0, 0]
workload8 = [50, 0, 60, 0, 0]

# Phase 3, increasing write ratio, increasing key skew
workload9 = [50, 0, 15, 0, 0]
workload10 = [50, 10, 10, 0, 0]
workload11 = [60, 20, 5, 0, 0]
workload12 = [60, 30, 0, 0, 0]

# Phase 4, balanced read ratio, increasing key skew, increasing workload skew
workload13 = [70, 50, 0, 0, 0]
workload14 = [100, 40, 0, 0, 0]
workload15 = [125, 30, 0, 0, 0]
workload16 = [150, 20, 0, 0, 0]

# Combine all the workloads into one parent list
workloadList = [
    workload1, workload2, workload3, workload4,
    workload5, workload6, workload7, workload8,
    workload9, workload10, workload11, workload12,
    workload13, workload14, workload15, workload16
]


def run_dynamic_workload(root_dir, exp_dir):
    for workload in workloadList:
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload
        shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
        generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                             numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity,
                             keySkew, workloadSkew, readRatio, locality, scopeRatio,
                             shellScriptPath, root_dir)
        execute_bash_script(shellScriptPath)

def run_single_workload(root_dir, exp_dir, index):
    keySkew, workloadSkew, readRatio, locality, scopeRatio = workloadList[index]
    shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
    generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity,
                         keySkew, workloadSkew, readRatio, locality, scopeRatio,
                         shellScriptPath, root_dir)
    execute_bash_script(shellScriptPath)


def main(root_dir, exp_dir):
    print(f"Root directory: {root_dir}")
    print(f"Experiment directory: {exp_dir}")

    run_dynamic_workload(root_dir, exp_dir)
    draw_throughput_comparison_plot(exp_dir)
    draw_latency_comparison_plot(exp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)
    print("5.2.2 dynamic throughput and latency results generated")