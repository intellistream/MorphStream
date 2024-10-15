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

function Per_Phase_Experiment() {{
    ResetParameters
    for ccStrategy in Adaptive Partitioning Offloading OpenNF CHC S6
    do
        ccStrategy=$ccStrategy
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


def get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy):
    return f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                  f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
                  f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
                  f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
                  f"doMVCC={doMVCC}/udfComplexity={udfComplexity}/throughput.csv"

def get_latency_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy):
    return f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
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








def draw_throughput_comparison_plot():
    systems = ['Adaptive', "Partitioning", "Offloading", 'OpenNF', 'CHC', 'S6']
    system_to_show = ['Nested', "Plain-1", "Plain-2", 'OpenNF', 'CHC', 'S6']
    system_throughputs = []
    colors = ['#AE48C8', '#de7104', '#0a79f0', '#F44040', '#15DB3F', '#E9AA18']
    hatches = ['\\\\', '///', '++', "xxx", "---", "oo"]
    hatch_colors = ['black', 'black', 'black', 'black', 'black', 'black']

    for system in systems:
        throughput_file_path = get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, system)
        system_throughputs.append(get_throughput_from_file(throughput_file_path) / 1e6 or 0)

    plt.figure(figsize=(7, 4.5))
    x_labels = systems
    # x_positions = np.arange(len(systems))
    x_positions = np.arange(len(systems)) * 0.8
    
    plt.bar(x_positions, system_throughputs, color=colors[:len(systems)], label=system_to_show, hatch=hatches[:len(systems)], edgecolor=hatch_colors[:len(systems)], width=0.4)

    plt.xticks(x_positions, system_to_show, fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel('State Management Strategies', fontsize=18)
    plt.ylabel('Throughput (Million req/sec)', fontsize=18)
    # plt.legend(fontsize=16)
    plt.legend(bbox_to_anchor=(0.5, 1.29), loc='upper center', ncol=3, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)
    ax = plt.gca()  
    yticks = ax.get_yticks()  # Automatically get y-axis tick positions
    ax.set_yticks(yticks) 

    plt.tight_layout()

    # plt.subplots_adjust(left=0.12, right=0.98, top=0.97, bottom=0.15)
    plt.subplots_adjust(left=0.12, right=0.98, top=0.82, bottom=0.15)

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.2.3_fineGrainedWorkload_range{numItems}_complexity{udfComplexity}.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))


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


def draw_latency_comparison_plot(max_points=1000):
    systems = ['Adaptive', "Partitioning", "Offloading", 'OpenNF', 'CHC', 'S6']
    system_to_show = ['Nested', "Plain-1", "Plain-2", 'OpenNF', 'CHC', 'S6']
    colors = ['#AE48C8', '#de7104', '#0a79f0', '#F44040', '#15DB3F', '#E9AA18']
    markers = ['o', 's', '^', 'D', 'x', 'v']
    system_latencies = []

    for system in systems:
        latency_file_path = get_latency_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, system)
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

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.2.3_fineGrainedWorkload_range{numItems}_complexity{udfComplexity}_latency.png'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))

    

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
ccStrategyList = ["Adaptive", "Partitioning", "Offloading", "OpenNF", "CHC", "S6"]
rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"

# Workload chars
expID = "5.2.3"
keySkew = 0
workloadSkew = 0
readRatio = 0
locality = 0
scopeRatio = 0


def exp():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID
    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         keySkew, workloadSkew, readRatio, locality, scopeRatio,
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)


if __name__ == "__main__":
    # exp()

    draw_throughput_comparison_plot()
    draw_latency_comparison_plot()
    print("Done")