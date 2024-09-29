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
                         keySkewList, workloadSkew, readRatioList, localityList, scopeRatio, 
                         script_path):
    
    keySkewList_str = " ".join(map(str, keySkewList))
    readRatioList_str = " ".join(map(str, readRatioList))
    localityList_str = " ".join(map(str, localityList))
    
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
  keySkew=0  # Default value, will be updated in loop
  workloadSkew={workloadSkew}
  readRatio=0  # Default value, will be updated in loop
  locality=0  # Default value, will be updated in loop
  scopeRatio={scopeRatio}
  enableTimeBreakdown=1
  enableMemoryFootprint=1
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
          --scopeRatio $scopeRatio \\
          --enableTimeBreakdown $enableTimeBreakdown \\
          --enableMemoryFootprint $enableMemoryFootprint
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
    --scopeRatio $scopeRatio \\
    --enableTimeBreakdown $enableTimeBreakdown \\
    --enableMemoryFootprint $enableMemoryFootprint
}}

function Per_Phase_Experiment() {{
  ResetParameters
  keySkewList=({keySkewList_str})
  readRatioList=({readRatioList_str})
  localityList=({localityList_str})

  for keySkew in "${{keySkewList[@]}}"
  do
    for readRatio in "${{readRatioList[@]}}"
    do
      for locality in "${{localityList[@]}}"
      do
        for ccStrategy in Offloading OpenNF CHC S6
        do
          keySkew=$keySkew
          readRatio=$readRatio
          locality=$locality
          ccStrategy=$ccStrategy

          runTStream
        done
      done
    done
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



def get_breakdown_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy):
    return f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                  f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
                  f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
                  f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
                  f"doMVCC={doMVCC}/udfComplexity={udfComplexity}/breakdown.csv"

def get_footprint_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy):
    return f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
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
    footprints = []
    with open(csv_file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            try:
                footprints.append(float(row[0]))
            except ValueError:
                print(f"Skipping invalid value: {row[0]}")
    return footprints


def draw_breakdown_comparison_plot():
    workload_feature_combinations = get_workload_feature_phases()
    
    bar_width = 0.5
    systemList = ['Offloading', 'OpenNF', 'CHC', 'S6']
    breakdownList = ['Parsing', 'Sync', 'Useful', 'Switching', 'Others']
    colors = ['white', 'white', 'white', 'white']
    hatches = ['\\\\\\', '////', '---', 'xxx', '+++']
    hatch_colors = ['#8c0b0b', '#0060bf', '#d97400', '#b313f2', '#047000']
    system_breakdown_data = {system: {breakdown: 0.0 for breakdown in breakdownList} for system in systemList}

    for workload in workload_feature_combinations:
        expID, keySkew, workloadSkew, readRatio, locality, scopeRatio = workload

        for system in systemList:
            outputFilePath = get_breakdown_file_path(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, system)
            breakdown_values = read_breakdown_from_file(outputFilePath)
            perPhaseParsingTime = breakdown_values[0] if (breakdown_values and breakdown_values[0] > 0) else 0
            perPhaseSyncTime = breakdown_values[1] if (breakdown_values and breakdown_values[1] > 0) else 0
            perPhaseUsefulTime = breakdown_values[2] if (breakdown_values and breakdown_values[2] > 0) else 0
            perPhaseSwitchingTime = breakdown_values[3] if (breakdown_values and breakdown_values[3] > 0) else 0
            perPhaseOthersTime = 0
            
            system_breakdown_data[system]['Parsing'] += perPhaseParsingTime
            system_breakdown_data[system]['Sync'] += perPhaseSyncTime
            system_breakdown_data[system]['Useful'] += perPhaseUsefulTime
            system_breakdown_data[system]['Switching'] += perPhaseSwitchingTime
            system_breakdown_data[system]['Others'] += perPhaseOthersTime

    parsing_times = [system_breakdown_data[system]['Parsing'] for system in systemList]
    sync_times = [system_breakdown_data[system]['Sync'] for system in systemList]
    useful_times = [system_breakdown_data[system]['Useful'] for system in systemList]
    switching_times = [system_breakdown_data[system]['Switching'] for system in systemList]
    others_times = [system_breakdown_data[system]['Others'] for system in systemList]

    plt.figure(figsize=(7, 4.5))
    fig, ax = plt.subplots()

    indices = np.arange(len(systemList))

    # Plot each category as a stacked horizontal bar
    p1 = ax.barh(indices, parsing_times, height=bar_width, label='Parsing', hatch='\\\\\\', color=colors[0], edgecolor=hatch_colors[0])
    p2 = ax.barh(indices, sync_times, height=bar_width, label='Sync', hatch='////', color=colors[1], edgecolor=hatch_colors[1], 
                 left=parsing_times)
    p3 = ax.barh(indices, useful_times, height=bar_width, label='Useful', hatch='---', color=colors[2], edgecolor=hatch_colors[2], 
                 left=np.add(parsing_times, sync_times))
    p4 = ax.barh(indices, switching_times, height=bar_width, label='Switching', hatch='xxx', color=colors[3], edgecolor=hatch_colors[3], 
                 left=np.add(np.add(parsing_times, sync_times), useful_times))
    # p5 = ax.barh(indices, others_times, height=bar_width, label='Others', hatch='+++', color='#047000', 
    #              left=np.add(np.add(np.add(parsing_times, sync_times), useful_times), switching_times))

    ax.set_yticks(indices)
    ax.set_yticklabels(systemList, fontsize=15)
    plt.xticks(fontsize=15)

    ax.set_xlabel('Execution Time (ms)', fontsize=18)
    ax.legend(bbox_to_anchor=(0.4, 1.23), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)

    plt.tight_layout()
    plt.subplots_adjust(left=0.2, right=0.98, top=0.85, bottom=0.15)

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.3_breakdown_range{numItems}_complexity{udfComplexity}.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))
    


def draw_footprint_comparison_plot():
    
    systems = ['Offloading', 'OpenNF', 'CHC', 'S6']
    colors = ['#AE48C8', '#F44040', '#15DB3F', '#E9AA18']
    markers = ['o', 's', '^', 'D']
    system_footprints = {system: [] for system in systems}

    for system in systems:
        footprint_file_path = get_footprint_file_path("5.3_phase1", 0, 0, 50, 75, 0, system)
        footprint_data = read_footprint_data(footprint_file_path)
        system_footprints[system].extend(footprint_data)

    plt.figure(figsize=(7, 4.5))
    fig, ax = plt.subplots()


    for i, system in enumerate(systems):
        footprints = system_footprints[system]
        indices = np.arange(len(footprints))
        plt.plot(indices, footprints, marker=markers[i], color=colors[i], label=system, markersize=6, markeredgecolor='black', markeredgewidth=1.5, markevery=100)

    plt.xticks(fontsize=15)
    plt.yticks(fontsize=15)

    plt.xlabel('Execution Time (10 ms)', fontsize=18)
    plt.ylabel('Memory Usage', fontsize=18)
    plt.legend(fontsize=16)
    ax.legend(bbox_to_anchor=(0.4, 1.23), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.97, bottom=0.15)
    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.3_footprint_range{numItems}_complexity{udfComplexity}.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))




vnfID = 11
numItems = 10000
numPackets = 100000
numInstances = 4
app = "nfv_test"

# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
ccStrategy = "Offloading"
doMVCC = 0
udfComplexity = 10
ccStrategyList = ["Offloading", "OpenNF", "CHC", "S6"]
rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"

# Workload chars
Phase1_expID = "5.3_phase1"
Phase1_keySkewList = [0]
Phase1_workloadSkewList = [0]
Phase1_readRatioList = [50]
Phase1_localityList = [75, 80, 90, 100]
Phase1_scopeRatioList = [0]

Phase2_expID = "5.3_phase2"
Phase2_keySkewList = [0, 50]
Phase2_workloadSkewList = [0]
Phase2_readRatioList = [75, 100]
Phase2_localityList = [0]
Phase2_scopeRatioList = [0]

Phase3_expID = '5.3_phase3'
Phase3_keySkewList = [0, 50]
Phase3_workloadSkewList = [0]
Phase3_readRatioList = [0, 25]
Phase3_localityList = [0]
Phase3_scopeRatioList = [0]

Phase4_expID = "5.3_phase4"
Phase4_keySkewList = [0, 50]
Phase4_workloadSkewList = [0]
Phase4_readRatioList = [25, 75]
Phase4_localityList = [0]
Phase4_scopeRatioList = [0]


def get_workload_feature_phases():
    # Generate combinations of workload features for each phase
    phase1_workload_combinations = [(Phase1_expID, *combination) for combination in itertools.product(
        Phase1_keySkewList, Phase1_workloadSkewList, Phase1_readRatioList, Phase1_localityList, Phase1_scopeRatioList)]
    
    phase2_workload_combinations = [(Phase2_expID, *combination) for combination in itertools.product(
        Phase2_keySkewList, Phase2_workloadSkewList, Phase2_readRatioList, Phase2_localityList, Phase2_scopeRatioList)]
    
    phase3_workload_combinations = [(Phase3_expID, *combination) for combination in itertools.product(
        Phase3_keySkewList, Phase3_workloadSkewList, Phase3_readRatioList, Phase3_localityList, Phase3_scopeRatioList)]
    
    phase4_workload_combinations = [(Phase4_expID, *combination) for combination in itertools.product(
        Phase4_keySkewList, Phase4_workloadSkewList, Phase4_readRatioList, Phase4_localityList, Phase4_scopeRatioList)]
    
    # Combine all phases into one list
    overall_workload_combinations = (
        phase1_workload_combinations + phase2_workload_combinations + 
        phase3_workload_combinations + phase4_workload_combinations
    )
    
    return overall_workload_combinations # Returns 16 five-tuples


def phase1():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % Phase1_expID
    generate_bash_script(app, Phase1_expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         Phase1_keySkewList, Phase1_workloadSkewList[0], Phase1_readRatioList, Phase1_localityList, Phase1_scopeRatioList[0], 
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)


def phase2():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % Phase2_expID
    generate_bash_script(app, Phase2_expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         Phase2_keySkewList, Phase2_workloadSkewList[0], Phase2_readRatioList, Phase2_localityList, Phase2_scopeRatioList[0], 
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)


def phase3():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % Phase3_expID
    generate_bash_script(app, Phase3_expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         Phase3_keySkewList, Phase3_workloadSkewList[0], Phase3_readRatioList, Phase3_localityList, Phase3_scopeRatioList[0], 
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)


def phase4():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % Phase4_expID
    generate_bash_script(app, Phase4_expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         Phase4_keySkewList, Phase4_workloadSkewList[0], Phase4_readRatioList, Phase4_localityList, Phase4_scopeRatioList[0], 
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)




if __name__ == "__main__":
    # phase1()
    # phase2()
    # phase3()
    # phase4()

    draw_breakdown_comparison_plot()
    draw_footprint_comparison_plot()

    print("Done")