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
    for ccStrategy in Partitioning Replication Offloading Proactive OpenNF CHC S6
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

def draw_throughput_plot_phase_1(expID, keySkew, workloadSkew, readRatio, localityRatioList, scopeRatio):
    colors = ['white', 'white', 'white', 'white', 'white', 'white', 'white']
    hatches = ['\\\\\\', '////', '--', '////', '--', '////', '--']
    hatch_colors = ['#8c0b0b', '#0060bf', '#d97400', '#D1D93B', '#D93BC0', '#43BF45', '#A243BF']

    data = {localityIndex: {} for localityIndex in localityRatioList}

    for localityIndex in localityRatioList:
        for ccStrategyIndex in ccStrategyList:
            outputFilePath = f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={localityIndex}/" \
                 f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategyIndex}/doMVCC={doMVCC}/udfComplexity={udfComplexity}/" \
                 "throughput.csv"
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                data[localityIndex][ccStrategyIndex] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                data[localityIndex][ccStrategyIndex] = None

    throughput_data = np.array([[data[localityIndex][ccStrategyIndex] if data[localityIndex][ccStrategyIndex] is not None else 0
                                 for ccStrategyIndex in ccStrategyList] for localityIndex in localityRatioList]) / 1e6

    bar_width = 0.05
    index = np.arange(len(localityRatioList))
    fig, ax = plt.subplots(figsize=(15, 5))

    for i, strategy in enumerate(ccStrategyList):
        ax.bar(index + i * bar_width, throughput_data[:, i], color=colors[i], hatch=hatches[i],
               edgecolor=hatch_colors[i], width=bar_width, label=ccStrategyList[i])

    ax.set_xticks([r + bar_width for r in range(len(localityRatioList))])
    ax.set_xticklabels(localityRatioList, fontsize=16)
    ax.tick_params(axis='y', labelsize=14)
    ax.set_xlabel('Locality Ratio', fontsize=18)
    ax.set_ylabel('Throughput (Million req/sec)', fontsize=18)
    handles = [Patch(facecolor=color, edgecolor=hatchcolor, hatch=hatch, label=label)
               for color, hatchcolor, hatch, label in zip(colors, hatch_colors, hatches, ccStrategyList)]
    ax.legend(handles=handles, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=7, fontsize=16)
    # plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'{expID}_localityRatio_range={numItems}_complexity={udfComplexity}.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))
    plt.savefig(os.path.join(figure_dir, figure_name))

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))




def draw_throughput_plot_phase_234(expID, keySkewList, workloadSkew, readRatioList, locality, scopeRatio):
    colors = ['white', 'white', 'white', 'white', 'white', 'white', 'white']
    hatches = ['\\\\\\', '////', '--', '////', '--', '////', '--']
    hatch_colors = ['#8c0b0b', '#0060bf', '#d97400', '#D1D93B', '#D93BC0', '#43BF45', '#A243BF']

    data = {(keySkew, readRatio): {} for keySkew in keySkewList for readRatio in readRatioList}

    for keySkew in keySkewList:
        for readRatio in readRatioList:
            for ccStrategyIndex in ccStrategyList:
                outputFilePath = f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                     f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/" \
                     f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                     f"puncInterval={puncInterval}/ccStrategy={ccStrategyIndex}/doMVCC={doMVCC}/udfComplexity={udfComplexity}/" \
                     "throughput.csv"
                try:
                    df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                    data[(keySkew, readRatio)][ccStrategyIndex] = df['Throughput'].iloc[0]
                except Exception as e:
                    print(f"Failed to read {outputFilePath}: {e}")
                    data[(keySkew, readRatio)][ccStrategyIndex] = None

    throughput_data = np.array([[data[(keySkew, readRatio)][ccStrategyIndex] if data[(keySkew, readRatio)][ccStrategyIndex] is not None else 0
                                 for ccStrategyIndex in ccStrategyList] for keySkew in keySkewList for readRatio in readRatioList]) / 1e6

    bar_width = 0.05
    index = np.arange(len(keySkewList) * len(readRatioList))
    fig, ax = plt.subplots(figsize=(15, 5))

    for i, strategy in enumerate(ccStrategyList):
        ax.bar(index + i * bar_width, throughput_data[:, i], color=colors[i], hatch=hatches[i],
               edgecolor=hatch_colors[i], width=bar_width, label=ccStrategyList[i])

    ax.set_xticks([r + bar_width for r in range(len(keySkewList) * len(readRatioList))])
    ax.set_xticklabels([f"key{ks}\nread{rr}" for ks in keySkewList for rr in readRatioList], fontsize=10)
    ax.tick_params(axis='y', labelsize=14)
    ax.set_xlabel('Key Skew and Read Ratio', fontsize=18)
    ax.set_ylabel('Throughput (Million req/sec)', fontsize=18)
    handles = [Patch(facecolor=color, edgecolor=hatchcolor, hatch=hatch, label=label)
               for color, hatchcolor, hatch, label in zip(colors, hatch_colors, hatches, ccStrategyList)]
    ax.legend(handles=handles, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=7, fontsize=16)

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'{expID}_keySkew_readRatio_range={numItems}_complexity={udfComplexity}.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))
    plt.savefig(os.path.join(figure_dir, figure_name))

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))


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
    systems = ['TransNFV', 'OpenNF', 'CHC', 'S6']
    colors = ['#AE48C8', '#F44040', '#15DB3F', '#E9AA18']
    markers = ['o', 's', '^', 'D']
    system_data = {system: [] for system in systems}

    for workload in workloadList:
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload
        
        transnfv_throughputs = []
        for ccStrategy in ["Partitioning", "Replication", "Offloading", "Proactive"]:
            outputFilePath = get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy)
            throughput = get_throughput_from_file(outputFilePath)
            if throughput:
                transnfv_throughputs.append(throughput)
        system_data['TransNFV'].append(max(transnfv_throughputs) / 1e6 if transnfv_throughputs else 0)

        outputFilePath_opennf = get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "OpenNF")
        system_data['OpenNF'].append(get_throughput_from_file(outputFilePath_opennf) / 1e6 or 0)

        outputFilePath_chc = get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "CHC")
        system_data['CHC'].append(get_throughput_from_file(outputFilePath_chc) / 1e6 or 0)

        outputFilePath_s6 = get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "S6")
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

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.2.2_dynamicWorkload_range{numItems}_complexity{udfComplexity}.pdf'
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
            latency_file = get_latency_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy)
            latencies = read_latencies(latency_file)
            avg_latency = sum(latencies) / len(latencies) if latencies else float('inf')
            if avg_latency < min_avg_latency:
                min_avg_latency = avg_latency
                transnfv_latencies = latencies

        system_latencies['TransNFV'].extend(transnfv_latencies)

        latency_file_opennf = get_latency_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "OpenNF")
        system_latencies['OpenNF'].extend(read_latencies(latency_file_opennf))

        latency_file_chc = get_latency_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "CHC")
        system_latencies['CHC'].extend(read_latencies(latency_file_chc))

        latency_file_s6 = get_latency_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "S6")
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

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.2.2_dynamicWorkload_range{numItems}_complexity{udfComplexity}_latency.png'
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
expID = "5.2.2"

# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
ccStrategy = "Offloading"
doMVCC = 1
udfComplexity = 10
ccStrategyList = ["Partitioning", "Replication", "Offloading", "Proactive", "OpenNF", "CHC", "S6"]
rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"


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


def run_dynamic_workload():
    for workload in workloadList:
        keySkew, workloadSkew, readRatio, locality, scopeRatio = workload
        shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID
        generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances,
                             numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity,
                             keySkew, workloadSkew, readRatio, locality, scopeRatio,
                             shellScriptPath)
        execute_bash_script(shellScriptPath)

def run_single_workload(index):
    keySkew, workloadSkew, readRatio, locality, scopeRatio = workloadList[index]
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID
    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity,
                         keySkew, workloadSkew, readRatio, locality, scopeRatio,
                         shellScriptPath)
    execute_bash_script(shellScriptPath)


if __name__ == "__main__":
    # run_dynamic_workload()

    # run_single_workload(0)
    # run_single_workload(1)
    # run_single_workload(2)
    # run_single_workload(3)
    # run_single_workload(4)
    # run_single_workload(5)
    # run_single_workload(6)
    # run_single_workload(7)
    # run_single_workload(8)
    # run_single_workload(9)
    # run_single_workload(10)
    # run_single_workload(11)
    # run_single_workload(12)
    # run_single_workload(13)
    # run_single_workload(14)
    # run_single_workload(15)

    # draw_throughput_comparison_plot()
    draw_latency_comparison_plot()
    print("Done")