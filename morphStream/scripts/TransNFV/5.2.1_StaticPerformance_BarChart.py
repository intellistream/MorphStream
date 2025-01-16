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
                         numTPGThreads, numOffloadThreads, puncInterval,
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

function Per_Phase_Experiment() {{
    ResetParameters
    for ccStrategy in Partitioning Replication Offloading OpenNF CHC S6
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


def get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy, vnfID, complexity):
    return f"{exp_dir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                  f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
                  f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
                  f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
                  f"doMVCC={doMVCC}/udfComplexity={complexity}/throughput.csv"

def get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy, vnfID, complexity):
    return f"{exp_dir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                  f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
                  f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
                  f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
                  f"doMVCC={doMVCC}/udfComplexity={complexity}/latency.csv"

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
    vnfNameList = ['FW', 'NAT', 'LB', 'TD', 'PD', 'PRADS', 'SBC', 'IPS', 'SCP', 'ATS']

    for i in range(len(workloadList)):
        workload = workloadList[i]
        complexity = compList[i]
        vnfID = i + 1
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload
        
        transnfv_throughputs = []
        for ccStrategy in ["Partitioning", "Replication", "Offloading"]:
            outputFilePath = get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy, vnfID, complexity)
            throughput = get_throughput_from_file(outputFilePath)
            if throughput:
                transnfv_throughputs.append(throughput)
        system_data['TransNFV'].append(max(transnfv_throughputs) / 1e6 if transnfv_throughputs else 0)

        outputFilePath_opennf = get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "OpenNF", vnfID, complexity)
        system_data['OpenNF'].append(get_throughput_from_file(outputFilePath_opennf) / 1e6 or 0)

        outputFilePath_chc = get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "CHC", vnfID, complexity)
        system_data['CHC'].append(get_throughput_from_file(outputFilePath_chc) / 1e6 or 0)

        outputFilePath_s6 = get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "S6", vnfID, complexity)
        system_data['S6'].append(get_throughput_from_file(outputFilePath_s6) / 1e6 or 0)

    plt.figure(figsize=(7, 4.5))
    x_labels = [f"{vnfNameList[i]}" for i in range(len(workloadList))]
    x_positions = np.arange(len(workloadList))
    
    for i, system in enumerate(systems):
        plt.plot(x_positions, system_data[system], marker=markers[i], color=colors[i], label=system, markersize=8, 
         markeredgecolor='black', markeredgewidth=1.5)

    plt.xticks(x_positions, x_labels, fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel('VNF Workload', fontsize=18)
    plt.ylabel('Throughput (Million req/sec)', fontsize=18)
    plt.legend(bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)
    ax = plt.gca()
    yticks = ax.get_yticks()
    yticks = yticks[yticks >= 0]  # Remove negative values
    ax.set_yticks(yticks)

    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)

    figure_name = f'5.2.1_staticWorkload.pdf'
    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))


def draw_throughput_bar_chart(exp_dir):
    # systems = ['TransNFV', 'OpenNF', 'CHC', 'S6']
    systems = ['TransNFV', 'S6', 'CHC', 'OpenNF']
    # colors = ['#AE48C8', '#F44040', '#15DB3F', '#E9AA18']
    # colors = ['#1b0073', '#5116f5', '#9193ff', '#d4ccff']
    colors = ['#03045e', '#0077b6', '#90e0ef', '#caf0f8']
    system_data = {system: [] for system in systems}
    vnfNameList = ['FW', 'NAT', 'LB', 'TD', 'PD', 'PRADS', 'SBC', 'IPS', 'SCP', 'ATS']

    for i in range(len(workloadList)):
        workload = workloadList[i]
        complexity = compList[i]
        vnfID = i + 1
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload

        for system in systems:
            if system == 'TransNFV':
                max_throughput = 0
                for ccStrategy in ["Partitioning", "Replication", "Offloading"]:
                    throughput_file = get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio,
                                                               locality, scopeRatio, ccStrategy, vnfID, complexity)
                    throughput = get_throughput_from_file(throughput_file)
                    if throughput:
                        max_throughput = max(max_throughput, throughput)

                system_data[system].append(max_throughput / 1e6 if max_throughput else 0)
            else:
                throughput_file = get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality,
                                                           scopeRatio, system, vnfID, complexity)
                system_data[system].append(get_throughput_from_file(throughput_file) / 1e6 or 0)

    # Plotting throughput bar chart
    x_positions = np.arange(len(vnfNameList))
    bar_width = 0.2
    plt.figure(figsize=(7, 4.5))

    for i, system in enumerate(systems):
        plt.bar(x_positions + i * bar_width, system_data[system], bar_width, color=colors[i], label=system)

    plt.xticks(x_positions + bar_width * (len(systems) - 1) / 2, vnfNameList, fontsize=14)
    plt.yticks(fontsize=15)
    plt.xlabel('VNF Workload', fontsize=18)
    plt.ylabel('Throughput (Million req/sec)', fontsize=18)
    plt.legend(bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', linestyle='--', linewidth=0.5, alpha=0.7)

    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)
    plt.savefig(os.path.join(figure_dir, '5.2.1_staticWorkload_throughput_bar.pdf'))



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
    vnfNameList = ['FW', 'NAT', 'LB', 'TD', 'PD', 'PRADS', 'SBC', 'IPS', 'SCP', 'ATS']

    for i in range(len(workloadList)):
        workload = workloadList[i]
        comp = compList[i]
        vnfID = i + 1
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload

        # TransNFV: choose the best strategy with the minimum average latency
        min_avg_latency = float('inf')
        for ccStrategy in ["Partitioning", "Replication", "Offloading"]:
            latency_file = get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio,
                                                 ccStrategy, vnfID, comp)
            latencies = read_latencies(latency_file)
            avg_latency = sum(latencies) / len(latencies) if latencies else float('inf')
            if avg_latency < min_avg_latency:
                min_avg_latency = avg_latency
        system_latencies['TransNFV'].append(min_avg_latency)

        # OpenNF
        latency_file_opennf = get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality,
                                                    scopeRatio, "OpenNF", vnfID, comp)
        latencies_opennf = read_latencies(latency_file_opennf)
        avg_latency_opennf = sum(latencies_opennf) / len(latencies_opennf) if latencies_opennf else float('inf')
        system_latencies['OpenNF'].append(avg_latency_opennf)

        # CHC
        latency_file_chc = get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio,
                                                 "CHC", vnfID, comp)
        latencies_chc = read_latencies(latency_file_chc)
        avg_latency_chc = sum(latencies_chc) / len(latencies_chc) if latencies_chc else float('inf')
        system_latencies['CHC'].append(avg_latency_chc)

        # S6
        latency_file_s6 = get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio,
                                                "S6", vnfID, comp)
        latencies_s6 = read_latencies(latency_file_s6)
        avg_latency_s6 = sum(latencies_s6) / len(latencies_s6) if latencies_s6 else float('inf')
        system_latencies['S6'].append(avg_latency_s6)

    # Plotting the average latency for each system across different workloads
    plt.figure(figsize=(7, 4.5))
    x_labels = [f"{vnfNameList[i]}" for i in range(len(workloadList))]
    x_positions = np.arange(len(workloadList))

    for i, system in enumerate(systems):
        plt.plot(x_positions, system_latencies[system], marker=markers[i], color=colors[i], label=system, markersize=8,
                 markeredgecolor='black', markeredgewidth=1.5)

    plt.xticks(x_positions, x_labels, fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel('VNF Workload', fontsize=18)
    plt.ylabel('Average Latency (s)', fontsize=18)
    plt.legend(bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)

    # Saving the figure
    figure_name = '5.2.1_staticWorkload_latency.png'
    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))

def draw_latency_bar_chart(exp_dir):
    systems = ['TransNFV', 'S6', 'CHC', 'OpenNF']
    # colors = ['#AE48C8', '#F44040', '#15DB3F', '#E9AA18']
    colors = ['#03045e', '#0077b6', '#90e0ef', '#caf0f8']
    system_data = {system: [] for system in systems}
    vnfNameList = ['FW', 'NAT', 'LB', 'TD', 'PD', 'PRADS', 'SBC', 'IPS', 'SCP', 'ATS']

    for i in range(len(workloadList)):
        workload = workloadList[i]
        complexity = compList[i]
        vnfID = i + 1
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload

        for system in systems:
            if system == 'TransNFV':
                min_avg_latency = float('inf')
                for ccStrategy in ["Partitioning", "Replication", "Offloading"]:
                    latency_file = get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality,
                                                         scopeRatio, ccStrategy, vnfID, complexity)
                    latencies = read_latencies(latency_file)
                    avg_latency = sum(latencies) / len(latencies) if latencies else float('inf')
                    if avg_latency < min_avg_latency:
                        min_avg_latency = avg_latency

                system_data[system].append(min_avg_latency if min_avg_latency != float('inf') else 0)
            else:
                latency_file = get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality,
                                                     scopeRatio, system, vnfID, complexity)
                latencies = read_latencies(latency_file)
                avg_latency = sum(latencies) / len(latencies) if latencies else 0
                system_data[system].append(avg_latency)

    # Plotting latency bar chart
    x_positions = np.arange(len(vnfNameList))
    bar_width = 0.2
    plt.figure(figsize=(7, 4.5))

    for i, system in enumerate(systems):
        plt.bar(x_positions + i * bar_width, system_data[system], bar_width, color=colors[i], label=system)

    plt.xticks(x_positions + bar_width * (len(systems) - 1) / 2, vnfNameList, fontsize=14)
    plt.yticks(fontsize=15)
    plt.xlabel('VNF Workload', fontsize=18)
    plt.ylabel('Average Latency (s)', fontsize=18)
    plt.legend(bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', linestyle='--', linewidth=0.5, alpha=0.7)

    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)
    plt.savefig(os.path.join(figure_dir, '5.2.1_staticWorkload_latency_bar.pdf'))



numItems = 10000
numPackets = 400000
numInstances = 4
app = "nfv_test"
expID = "5.2.1"

# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
doMVCC = 0
ccStrategyList = ["Partitioning", "Replication", "Offloading", "OpenNF", "CHC", "S6"]


# KeySkew, WorkloadSkew, ReadRatio, Locality, ScopeRatio
workload1 = [0, 0, 50, 100, 100] # FW
comp1 = 1
workload2 = [0, 0, 80, 0, 50] # NAT
comp2 = 2
workload3 = [50, 0, 50, 0, 0] # LB
comp3 = 4
workload4 = [0, 0, 50, 0, 20] # TD
comp4 = 10
workload5 = [0, 0, 50, 0, 0] # PD
comp5 = 6
workload6 = [0, 0, 50, 0, 0] # PRADS
comp6 = 2
workload7 = [0, 0, 60, 0, 0] # SBC
comp7 = 4
workload8 = [0, 0, 60, 0, 0] # IPS
comp8 = 5
workload9 = [0, 0, 0, 0, 0] # SCP
comp9 = 3
workload10 = [0, 0, 60, 0, 0] # ATS
comp10 = 5

workloadList = [
    workload1, workload2, workload3, workload4, workload5, workload6, workload7, workload8, workload9, workload10
]

compList = [
    comp1, comp2, comp3, comp4, comp5, comp6, comp7, comp8, comp9, comp10
]


def run_dynamic_workload(root_dir, exp_dir):
    shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
    for i in range (0, 10):
        vnfID = i+1
        workload = workloadList[i]
        complexity = compList[i]
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload
        generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                             numTPGThreads, numOffloadThreads, puncInterval, doMVCC, complexity,
                             keySkew, workloadSkew, readRatio, locality, scopeRatio,
                             shellScriptPath, root_dir)
        execute_bash_script(shellScriptPath)

def run_single_workload(root_dir, exp_dir, index):
    keySkew, workloadSkew, readRatio, locality, scopeRatio = workloadList[index]
    complexity = compList[index]
    vnfID = index + 1
    shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
    generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, doMVCC, complexity,
                         keySkew, workloadSkew, readRatio, locality, scopeRatio,
                         shellScriptPath, root_dir)
    execute_bash_script(shellScriptPath)


def main(root_dir, exp_dir):
    print(f"Root directory: {root_dir}")
    print(f"Experiment directory: {exp_dir}")

    # run_dynamic_workload(root_dir, exp_dir)
    draw_throughput_bar_chart(exp_dir)
    draw_latency_bar_chart(exp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)