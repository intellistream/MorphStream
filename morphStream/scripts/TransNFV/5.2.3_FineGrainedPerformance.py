import argparse
import subprocess
import os
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import csv


def generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy,
                         doMVCC, udfComplexity,
                         keySkew, workloadSkew, readRatio, locality, scopeRatio,
                         shell_script_path, root_dir):
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
    for ccStrategy in Nested Partitioning Offloading OpenNF CHC S6
    do
        ccStrategy=$ccStrategy
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
    systems = ['Nested', "Partitioning", "Offloading", 'OpenNF', 'CHC', 'S6']
    system_to_show = ['Nested', "Plain-1", "Plain-2", 'OpenNF', 'CHC', 'S6']
    system_throughputs = []
    # colors = ['#AE48C8', '#de7104', '#0a79f0', '#F44040', '#15DB3F', '#E9AA18']
    colors = ['#03045e', '#023e8a', '#0077b6', '#00b4d8', '#90e0ef', '#caf0f8']
    # colors = ['white', 'white', 'white', 'white', 'white', 'white']
    # hatch_colors = ['#8c0b0b', '#0060bf', '#d97400', '#b313f2', '#0060bf', '#d97400']
    hatches = ['o', '+', '//', "xx", "--", "\\\\"]
    hatch_colors = ['black', 'black', 'black', 'black', 'black', 'black']

    for system in systems:
        throughput_file_path = get_throughput_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, system)
        system_throughputs.append(get_throughput_from_file(throughput_file_path) / 1e6 or 0)

    plt.figure(figsize=(7, 4.5))
    x_labels = systems
    # x_positions = np.arange(len(systems))
    x_positions = np.arange(len(systems)) * 0.8
    
    # plt.bar(x_positions, system_throughputs, color=colors[:len(systems)], label=system_to_show, hatch=hatches[:len(systems)], edgecolor=hatch_colors[:len(systems)], width=0.35)
    plt.bar(x_positions, system_throughputs, color=colors[:len(systems)], label=system_to_show, width=0.4)

    plt.xticks(x_positions, system_to_show, fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel('State Management Strategies', fontsize=18)
    plt.ylabel('Throughput (Million req/sec)', fontsize=18)
    plt.legend(bbox_to_anchor=(0.5, 1.29), loc='upper center', ncol=3, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)
    ax = plt.gca()  
    yticks = ax.get_yticks()  # Automatically get y-axis tick positions
    ax.set_yticks(yticks) 

    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.82, bottom=0.15)

    figure_name = f'5.2.3_fineGrainedWorkload.pdf'
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
    """Downsamples the data to a maximum of max_points points."""
    if len(data) > max_points:
        indices = np.linspace(0, len(data) - 1, max_points, dtype=int)
        return data[indices]
    return data


def draw_latency_comparison_plot(exp_dir, max_points=1000):
    systems = ['Nested', "Partitioning", "Offloading", 'OpenNF', 'CHC', 'S6']
    system_to_show = ['Nested', "Plain-1", "Plain-2", 'OpenNF', 'CHC', 'S6']
    colors = ['#AE48C8', '#de7104', '#0a79f0', '#F44040', '#15DB3F', '#E9AA18']
    markers = ['o', 's', '^', 'D', 'x', 'v']
    system_latencies = []

    for system in systems:
        latency_file_path = get_latency_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, system)
        system_latencies.append(read_latencies(latency_file_path))

    plt.figure(figsize=(7, 4.5))

    for i, system in enumerate(systems):
        sorted_latencies = np.sort(system_latencies[i])
        downsampled_latencies = downsample_data(sorted_latencies, max_points)
        cdf = np.arange(1, len(downsampled_latencies) + 1) / len(downsampled_latencies)
        marker_positions = list(range(0, 1000, 100)) + [999]
        plt.plot(downsampled_latencies, cdf, marker=markers[i], color=colors[i], label=system_to_show[i], markersize=8,
                 markeredgecolor='black', markeredgewidth=1.5, markevery=marker_positions)

    plt.xticks(fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel('Latency (s)', fontsize=18)
    plt.ylabel('Cumulative Percent (%)', fontsize=18)
    plt.legend(bbox_to_anchor=(0.5, 1.29), loc='upper center', ncol=3, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.82, bottom=0.15)

    figure_name = f'5.2.3_fineGrainedWorkload_latency.png'
    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))

    

vnfID = 11
numItems = 10000
numPackets = 400000
numInstances = 4
app = "nfv_test"

# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
ccStrategy = "Offloading"
doMVCC = 0
udfComplexity = 10
ccStrategyList = ["Nested", "Partitioning", "Offloading", "OpenNF", "CHC", "S6"]

# Workload chars
expID = "5.2.3"
keySkew = 0
workloadSkew = 0
readRatio = 0
locality = 0
scopeRatio = 0


def run_fine_grained_exp(root_dir, exp_dir):
    shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
    print(f"Shell script path: {shellScriptPath}")
    generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         keySkew, workloadSkew, readRatio, locality, scopeRatio,
                         shellScriptPath, root_dir)
    
    execute_bash_script(shellScriptPath)


def main(root_dir, exp_dir):
    print(f"Root directory: {root_dir}")
    print(f"Experiment directory: {exp_dir}")

    # run_fine_grained_exp(root_dir, exp_dir)
    draw_throughput_comparison_plot(exp_dir)
    # draw_latency_comparison_plot(exp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)
    print("5.2.3 fine-grained variation results generated")