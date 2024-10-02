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
    systems = ['Adaptive', "Partitioning", "Offloading", 'OpenNF', 'CHC', 'S6']
    colors = ['#AE48C8', '#F44040', '#15DB3F', '#E9AA18', '#15DB3F', '#E9AA18']
    markers = ['o', 's', '^', 'D', '^', 'D']
    system_data = {system: [] for system in systems}
        
    outputFilePath_adaptive = get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "Adaptive")
    system_data['Adaptive'].append(get_throughput_from_file(outputFilePath_adaptive) / 1e6 or 0)

    outputFilePath_partitioning = get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "Partitioning")
    system_data['Partitioning'].append(get_throughput_from_file(outputFilePath_partitioning) / 1e6 or 0)

    outputFilePath_offloading = get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "Offloading")
    system_data['Offloading'].append(get_throughput_from_file(outputFilePath_offloading) / 1e6 or 0)

    outputFilePath_opennf = get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "OpenNF")
    system_data['OpenNF'].append(get_throughput_from_file(outputFilePath_opennf) / 1e6 or 0)

    outputFilePath_chc = get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "CHC")
    system_data['CHC'].append(get_throughput_from_file(outputFilePath_chc) / 1e6 or 0)

    outputFilePath_s6 = get_throughput_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "S6")
    system_data['S6'].append(get_throughput_from_file(outputFilePath_s6) / 1e6 or 0)

    plt.figure(figsize=(7, 4.5))
    x_labels = systems
    x_positions = np.arange(len(systems))
    
    for i, system in enumerate(systems):
        plt.plot(x_positions, system_data[system], marker=markers[i], color=colors[i], label=system, markersize=8, 
         markeredgecolor='black', markeredgewidth=1.5)

    plt.xticks(x_positions, x_labels, fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel('Dynamic Workload Phases', fontsize=18)
    plt.ylabel('Throughput (Million req/sec)', fontsize=18)
    # plt.legend(fontsize=16)
    plt.legend(bbox_to_anchor=(0.45, 1.23), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)
    ax = plt.gca()  
    yticks = ax.get_yticks()  # Automatically get y-axis tick positions
    ax.set_yticks(yticks) 

    plt.tight_layout()

    # plt.subplots_adjust(left=0.12, right=0.98, top=0.97, bottom=0.15)
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)

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
                latency = float(row[0])
                if latency < 200:
                    latencies.append(float(row[0]))
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
    colors = ['#AE48C8', '#F44040', '#15DB3F', '#E9AA18', '#15DB3F', '#E9AA18']
    markers = ['o', 's', '^', 'D', '^', 'D']
    system_latencies = {system: [] for system in systems}

    latency_file_adaptive = get_latency_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "Adaptive")
    system_latencies['Adaptive'].extend(read_latencies(latency_file_adaptive))

    latency_file_partitioning = get_latency_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "Partitioning")
    system_latencies['Partitioning'].extend(read_latencies(latency_file_partitioning))

    latency_file_offloading = get_latency_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, "Offloading")
    system_latencies['Offloading'].extend(read_latencies(latency_file_offloading))

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
        # plt.plot(downsampled_latencies, cdf, marker=markers[i], color=colors[i], label=system, markersize=6, markeredgecolor='black', markeredgewidth=1.5, markevery=100)
        if system == 'TransNFV':
            plt.plot(downsampled_latencies, cdf, marker=markers[i], color=colors[i], label=system, markersize=6, markeredgecolor='black', markeredgewidth=1.5, markevery=(50, 100))
        else:
            plt.plot(downsampled_latencies, cdf, marker=markers[i], color=colors[i], label=system, markersize=6, markeredgecolor='black', markeredgewidth=1.5, markevery=100)

    plt.xticks(fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel('Latency (us)', fontsize=18)
    plt.ylabel('CDF', fontsize=18)
    # plt.legend(fontsize=16)
    plt.legend(bbox_to_anchor=(0.45, 1.23), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    plt.tight_layout()
    # plt.subplots_adjust(left=0.12, right=0.98, top=0.97, bottom=0.15)
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)

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
    exp()

    # draw_throughput_comparison_plot()
    # draw_latency_comparison_plot()
    print("Done")