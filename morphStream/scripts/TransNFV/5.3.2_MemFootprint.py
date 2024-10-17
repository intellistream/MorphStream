import argparse
import subprocess
import os
import threading
import matplotlib.pyplot as plt
import csv


def generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy,
                         doMVCC, udfComplexity,
                         keySkew, workloadSkew, readRatio, locality, scopeRatio,
                         workloadInterval, monitorWindowSize, hardcodeSwitch,
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
  monitorWindowSize={monitorWindowSize}
  workloadInterval={workloadInterval}
  hardcodeSwitch={hardcodeSwitch}
}}

function runTStream() {{
  echo "java -Xms1g -Xmx100g -Xss10M -jar {root_dir}/morphStream/morph-clients/target/morph-clients-0.1.jar \\
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
  java -Xms1g -Xmx100g -Xss10M -jar {root_dir}/morphStream/morph-clients/target/morph-clients-0.1.jar \\
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

def get_footprint_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy):
    return f"{exp_dir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
           f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
           f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
           f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
           f"doMVCC={doMVCC}/udfComplexity={udfComplexity}/footprint.csv"

def draw_footprint_comparison_plot(exp_dir):
    systems = ['OpenNF', 'CHC', 'S6', 'Adaptive']
    displayedSystems = ['OpenNF', 'CHC', 'S6', 'TransNFV']
    colors = ['#AE48C8', '#F44040', '#15DB3F', '#E9AA18']
    markers = ['o', 's', '^', 'D']
    system_footprints = {system: [] for system in systems}
    system_timestamps = {system: [] for system in systems}

    for system in systems:
        footprint_file_path = get_footprint_file_path(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio,
                                                      system)
        timestamps = []
        footprints = []
        with open(footprint_file_path, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                try:
                    # Check if the row has at least 2 elements
                    if len(row) < 2:
                        print(f"Error reading file {footprint_file_path}. Skipping row with insufficient columns: {row}")
                        continue

                    # Process the row as expected
                    timestamp = float(row[0].strip()) / 1000000
                    footprint = float(row[1].strip())
                    timestamps.append(timestamp)
                    footprints.append(footprint)

                except ValueError as e:
                    print(f"Error parsing file: {footprint_file_path} line: {row}. Exception: {e}")

        system_footprints[system].extend(footprints)
        system_timestamps[system].extend(timestamps)

    fig, ax = plt.subplots(figsize=(7, 4.5))

    for i, system in enumerate(systems):
        timestamps = system_timestamps[system]
        footprints = system_footprints[system]
        plt.plot(timestamps, footprints, marker=markers[i], color=colors[i], label=displayedSystems[i], markersize=6,
                 markeredgecolor='black', markeredgewidth=1.5, markevery=10)

    plt.xticks(fontsize=15)
    plt.yticks(fontsize=15)

    plt.xlabel('Execution Time (ms)', fontsize=18)  # You can change the label as appropriate
    plt.ylabel('Memory Usage (GB)', fontsize=18)
    plt.legend(fontsize=16)
    ax.legend(bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)

    figure_name = f'5.3.2.pdf'
    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))


vnfID = 11
numItems = 1000
numPackets = 800000
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
udfComplexity = 3

# Workload chars
expID = "5.3"
keySkew = 0
workloadSkew = 0
readRatio = 0
locality = 0
scopeRatio = 0


def run_mem_footprint_analysis(root_dir, exp_dir):
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

    # run_mem_footprint_analysis(root_dir, exp_dir)
    draw_footprint_comparison_plot(exp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)
    print("Preliminary study results generated")