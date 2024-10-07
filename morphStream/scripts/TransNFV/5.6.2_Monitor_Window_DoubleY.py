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
                         monitorWindowSize, workloadInterval, hardcodeSwitch,
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
  monitorWindowSize={monitorWindowSize}
  workloadInterval={workloadInterval}
  hardcodeSwitch={hardcodeSwitch}
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
          --monitorWindowSize $monitorWindowSize \\
          --workloadInterval $workloadInterval \\
          --hardcodeSwitch $hardcodeSwitch
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
    --monitorWindowSize $monitorWindowSize \\
    --workloadInterval $workloadInterval \\
    --hardcodeSwitch $hardcodeSwitch
}}

function Per_Phase_Experiment() {{
    ResetParameters
    for workloadInterval in 10000 20000 50000 100000 200000
    do
        for monitorWindowSize in 10000 20000 50000 100000
        do
            runTStream
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





def plot_complex_study():
    throughput_data_max_raw = {workloadInterval: 0 for workloadInterval in workloadIntervalList}
    for workloadInterval in workloadIntervalList:
        outputFilePath = f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/" \
                 f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategy}/doMVCC={doMVCC}/udfComplexity={udfComplexity}/" \
                 f"workloadInterval={workloadInterval}/monitorWindowSize={defaultWindowSize}/hardcodeSwitch=1/" \
                 "throughput.csv"
        # Read the CSV file
        try:
            df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
            throughput_data_max_raw[workloadInterval] = df['Throughput'].iloc[0]
        except Exception as e:
            print(f"Failed to read {outputFilePath}: {e}")
            throughput_data_max_raw[workloadInterval] = None

    throughput_data_max = np.array([throughput_data_max_raw[workloadInterval] if throughput_data_max_raw[workloadInterval] is not None else 0
                                  for workloadInterval in workloadIntervalList]) / 1e6


    throughput_data_raw = {workloadInterval: {} for workloadInterval in workloadIntervalList}
    throughput_data_delta = {workloadInterval: {} for workloadInterval in workloadIntervalList}

    for workloadInterval in workloadIntervalList:
        for monitorWindowSize in monitorWindowSizeList:
            outputFilePath = f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/" \
                 f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategy}/doMVCC={doMVCC}/udfComplexity={udfComplexity}/" \
                 f"workloadInterval={workloadInterval}/monitorWindowSize={monitorWindowSize}/hardcodeSwitch=0/" \
                 "throughput.csv"

            # Read the CSV file
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                throughput_data_raw[workloadInterval][monitorWindowSize] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                throughput_data_raw[workloadInterval][monitorWindowSize] = None

    throughput_data = np.array([[throughput_data_raw[workloadInterval][monitorWindowSize] if throughput_data_raw[workloadInterval][monitorWindowSize] is not None else 0
                                 for monitorWindowSize in monitorWindowSizeList] for workloadInterval in workloadIntervalList]) / 1e6
    
    # Compute the delta throughput
    for i, workloadInterval in enumerate(workloadIntervalList):
        for j, monitorWindowSize in enumerate(monitorWindowSizeList):
            throughput_data_delta[workloadInterval][monitorWindowSize] = throughput_data_max[i] - throughput_data[i][j]


    


    switch_data = {workloadInterval: {} for workloadInterval in workloadIntervalList}

    for workloadInterval in workloadIntervalList:
        for monitorWindowSize in monitorWindowSizeList:
            outputFilePath = f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                 f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/locality={locality}/" \
                 f"scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/numOffloadThreads={numOffloadThreads}/" \
                 f"puncInterval={puncInterval}/ccStrategy={ccStrategy}/doMVCC={doMVCC}/udfComplexity={udfComplexity}/" \
                 f"workloadInterval={workloadInterval}/monitorWindowSize={monitorWindowSize}/hardcodeSwitch=0/" \
                 "breakdown.csv"

            # Read the CSV file
            try:
                df = pd.read_csv(outputFilePath, header=None, names=['Parse', 'Sync', 'Useful', 'Switch'])
                switch_data[workloadInterval][monitorWindowSize] = df['Switch'].iloc[0]
            except Exception as e:
                print(f"Failed to read {outputFilePath}: {e}")
                switch_data[workloadInterval][monitorWindowSize] = None

    print(switch_data)

    bar_width = 0.2
    x = np.arange(len(workloadIntervalList))
    colors = ['#0060bf', '#8c0b0b', '#d97400', '#7812a1']
    hatches = ['////', '---', '\\\\', 'xxx']
    markers = ['o', 's', 'D', '^']
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(7, 4.5), sharex=True)

    for i, window in enumerate(monitorWindowSizeList):
        ax2.bar(x + i * bar_width, [switch_data[interval][window] for interval in workloadIntervalList], 
                width=bar_width, label=f'W = {window // 1000}K', color=colors[i], hatch=hatches[i])



    ax2.set_ylabel('Overhead\n(ms)', color='black', fontsize=15)
    ax2.set_xticks(x + bar_width * 1.5)
    # ax2.set_xticklabels(workloadIntervalList, fontsize=16)
    ax2.set_xticklabels([f'{interval // 1000}K' for interval in workloadIntervalList], fontsize=16)

    ax2.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    # Plotting line chart for delta throughput
    for i, window in enumerate(monitorWindowSizeList):
        ax1.plot(x + bar_width * 1.5, throughput_data[:, i], marker=markers[i], label=f'W = {window // 1000}K', 
                color=colors[i], markersize=8, linewidth=2)

        
    ax1.set_ylabel('Throughput (M Req/s)\n[Optimal-Actual]', color='black', fontsize=15)
    ax1.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    ax1.tick_params(axis='y', labelsize=13)
    ax2.tick_params(axis='y', labelsize=13)

    ax1.get_yaxis().set_label_coords(-0.1, 0.65)
    ax2.get_yaxis().set_label_coords(-0.1, 0.4)

    plt.xlabel('Workload Intervals',fontsize=18, labelpad=6)
    handles, labels = ax1.get_legend_handles_labels()  # Get legend from ax1
    fig.legend(handles, labels, bbox_to_anchor=(0.54, 1), loc='upper center', ncol=4, fontsize=14, columnspacing=0.5)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.subplots_adjust(left=0.15, right=0.98, top=0.85, bottom=0.15)
    

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.6.2_windowSizeComplex_range={numItems}_complexity={udfComplexity}.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))



    

vnfID = 11
numItems = 100
numPackets = 400000
numInstances = 4
app = "nfv_test"
defaultWindowSize = 10000
workloadIntervalList  = [10000, 20000, 50000, 100000, 200000]
monitorWindowSizeList = [10000, 20000, 50000, 100000]

# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
ccStrategy = "Adaptive"
doMVCC = 0
udfComplexity = 10
rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"

# Workload chars
expID = "5.6.2"
keySkew = 0 # Default parameters. The actual workload variation are embedded in the shared dynamic workload file
workloadSkew = 0
readRatio = 0
locality = 0
scopeRatio = 0
monitorWindowSize = 1000
workloadInterval = 10000


def runHardcodeSwitch():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID
    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         keySkew, workloadSkew, readRatio, locality, scopeRatio, monitorWindowSize, workloadInterval, 1,
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)


def runNoHardcodeSwitch():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID
    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         keySkew, workloadSkew, readRatio, locality, scopeRatio, monitorWindowSize, workloadInterval, 0,
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)




if __name__ == "__main__":
    # runHardcodeSwitch()
    # runNoHardcodeSwitch()
    plot_complex_study()
    # plot_keyskew_throughput_figure()
    print("Done")