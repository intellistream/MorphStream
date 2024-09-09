import subprocess
import os
import time
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import csv

def generate_bash_script(app, checkpointInterval, numTPGThreads, scheduler, defaultScheduler, complexity, NUM_ITEMS, rootFilePath,
                         totalEvents, nfvExperimentPath, communicationChoice, numInstances, numOffloadThreads, offloadLockNum,
                         ccStrategy, workloadPattern, enableTimeBreakdown, instancePatternPunctuation, expID, script_path, enableHardcodeCCSwitch):
    script_content = f"""#!/bin/bash

function ResetParameters() {{
  app="{app}"
  checkpointInterval={checkpointInterval}
  numTPGThreads={numTPGThreads}
  scheduler="{scheduler}"
  NUM_ITEMS={NUM_ITEMS}
  totalEvents={totalEvents}
  nfvExperimentPath="{nfvExperimentPath}"
  communicationChoice={communicationChoice}
  numInstances={numInstances}
  numOffloadThreads={numOffloadThreads}
  offloadLockNum={offloadLockNum}
  ccStrategy={ccStrategy}
  workloadPattern={workloadPattern}
  enableTimeBreakdown={enableTimeBreakdown}
  instancePatternPunctuation={instancePatternPunctuation}
  expID="{expID}"
  enableHardcodeCCSwitch="{enableHardcodeCCSwitch}"
}}

function runTStream() {{
  echo "java -Xms100g -Xmx100g -jar -d64 /home/shuhao/DB4NFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
          --app $app \\
          --NUM_ITEMS $NUM_ITEMS \\
          --numTPGThreads $numTPGThreads \\
          --scheduler $scheduler \\
          --checkpoint_interval $checkpointInterval \\
          --totalEvents $totalEvents \\
          --nfvExperimentPath $nfvExperimentPath \\
          --communicationChoice $communicationChoice \\
          --numInstances $numInstances \\
          --numOffloadThreads $numOffloadThreads \\
          --offloadLockNum $offloadLockNum \\
          --ccStrategy $ccStrategy \\
          --workloadPattern $workloadPattern \\
          --enableTimeBreakdown $enableTimeBreakdown \\
          --instancePatternPunctuation $instancePatternPunctuation \\
          --expID $expID \\
          --enableHardcodeCCSwitch $enableHardcodeCCSwitch
          "
  java -Xms100g -Xmx100g -Xss10M -jar -d64 /home/shuhao/DB4NFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
    --app $app \\
    --NUM_ITEMS $NUM_ITEMS \\
    --numTPGThreads $numTPGThreads \\
    --scheduler $scheduler \\
    --checkpoint_interval $checkpointInterval \\
    --totalEvents $totalEvents \\
    --nfvExperimentPath $nfvExperimentPath \\
    --communicationChoice $communicationChoice \\
    --numInstances $numInstances \\
    --numOffloadThreads $numOffloadThreads \\
    --offloadLockNum $offloadLockNum \\
    --ccStrategy $ccStrategy \\
    --workloadPattern $workloadPattern \\
    --enableTimeBreakdown $enableTimeBreakdown \\
    --instancePatternPunctuation $instancePatternPunctuation \\
    --expID $expID \\
    --enableHardcodeCCSwitch $enableHardcodeCCSwitch
}}

function baselinePattern() {{
  ResetParameters
  for workloadPattern in 0 1 2 3
  do
    for ccStrategy in 0 1 2 3 4 5 6
    do
      runTStream
    done
  done
}}

baselinePattern
ResetParameters
"""

    with open(script_path, "w") as file:
        file.write(script_content)

    # Make the script executable
    os.chmod(script_path, 0o755)

def stream_reader(pipe, pipe_name):
    with pipe:
        for line in iter(pipe.readline, ''):
            print(f"{pipe_name}: {line.strip()}")

def execute_bash_script(script_path):
    print(f"Executing bash script: {script_path}")

    # Execute the bash script
    process = subprocess.Popen(["bash", script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Start threads to read stdout and stderr
    stdout_thread = threading.Thread(target=stream_reader, args=(process.stdout, "STDOUT"))
    stderr_thread = threading.Thread(target=stream_reader, args=(process.stderr, "STDERR"))
    stdout_thread.start()
    stderr_thread.start()

    # Wait for the process to complete
    process.wait()
    stdout_thread.join()
    stderr_thread.join()

    if process.returncode != 0:
        print(f"Bash script finished with errors.")
    else:
        print(f"Bash script completed successfully.")


def read_throughput_values(root_dir, system):
    patterns = ["loneOperative", "sharedReaders", "sharedWriters", "mutualInteractive"]
    punctuations = [1, 2, 3, 4]

    throughput_values = []

    for pattern in patterns:
        for punc in punctuations:
            file_path = os.path.join(root_dir, f"{pattern}/{system}/punc_{punc}.csv")
            try:
                with open(file_path, 'r') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        throughput_values.append(float(row[-1]))  # Append the last element as throughput value
            except FileNotFoundError:
                print(f"File not found: {file_path}")
            except ValueError:
                print(f"Invalid data in file: {file_path}")

    return throughput_values

def calculate_transnfv(throughput_lists):
    transnfv_throughput = []
    for values in zip(*throughput_lists):
        transnfv_throughput.append(max(values))
    return transnfv_throughput

def calculate_s6(replication_throughput, s6_throughput):
    s6_min_throughput = []
    for rep_val, s6_val in zip(replication_throughput, s6_throughput):
        s6_min_throughput.append(min(rep_val, s6_val))
    return s6_min_throughput

def plot_dynamic_throughput_linechart():
    # Root directory containing the CSV files
    root_dir = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.2.2/throughput"

    throughput_partitioning = read_throughput_values(root_dir, "Partitioning")
    throughput_replication = read_throughput_values(root_dir, "Replication")
    throughput_offloading = read_throughput_values(root_dir, "Offloading")
    throughput_preemptive = read_throughput_values(root_dir, "Preemptive")
    throughput_opennf = read_throughput_values(root_dir, "OpenNF")
    throughput_chc = read_throughput_values(root_dir, "CHC")
    throughput_s6_original = read_throughput_values(root_dir, "S6")

    # Calculate TransNFV and S6 throughputs
    throughput_transnfv = calculate_transnfv([
        throughput_partitioning,
        throughput_replication,
        throughput_offloading,
        throughput_preemptive,
        throughput_s6_original
    ])

    throughput_s6 = calculate_s6(throughput_replication, throughput_s6_original)

    # Generate the x-axis values based on the number of throughput values
    punctuations = np.arange(1, len(throughput_transnfv) + 1)

    # Plot the data
    plt.figure(figsize=(8, 6))

    plt.plot(punctuations, throughput_transnfv, marker='o', markersize=10, linestyle='-', color='b', label='TransNFV')
    plt.plot(punctuations, throughput_opennf, marker='s', markersize=10, linestyle='-', color='g', label='OpenNF')
    plt.plot(punctuations, throughput_chc, marker='^', markersize=10, linestyle='-', color='r', label='CHC')
    plt.plot(punctuations, throughput_s6, marker='d', markersize=10, linestyle='-', color='purple', label='S6')

    # Adding title and labels
#     plt.title('Throughput Changes Over Time for Different Systems')
    plt.xlabel('Punctuation', fontsize=24)
    plt.ylabel("Throughput (10^6 packet/sec)", fontsize=24)
    plt.xticks(punctuations, fontsize=20)  # Ensure all punctuations are shown on the x-axis
    plt.yticks(fontsize=20)  # Set y-axis number sizes to 14
    plt.legend(loc='upper right', fontsize=24)

    # Show grid
    plt.grid(True)
    plt.tight_layout()
    script_dir = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/"
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, '5.2.2_Throughput.pdf'))
    plt.savefig(os.path.join(figure_dir, '5.2.2_Throughput.png'))
    print("5.2.2 throughput figure generated.")

def plot_latency_CDF():
    root_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.2.2/latency"
    patterns = ["loneOperative", "sharedReaders", "sharedWriters", "mutualInteractive"]
    cc_strategies = ["Partitioning", "Replication", "Offloading", "OpenNF", "CHC", "S6"]

    final_latency_data = {strategy: [] for strategy in ["TransNFV", "S6", "OpenNF", "CHC"]}

    for pattern in patterns:
        latency_data = {}
        avg_latencies = {}

        # Read and store latency data
        for strategy in cc_strategies:
            file_path = os.path.join(root_directory, pattern, f"{strategy}.csv")
            latency_data[strategy] = []
            try:
                with open(file_path, 'r') as file:
                    reader = csv.reader(file)
                    next(reader)  # Skip the header
                    for row in reader:
                        latency_value = float(row[0])
                        if latency_value < 0:
                            print(f"Negative latency value found: {latency_value} in file {file_path}")
                        elif latency_value <= 300:  # Ignore latency values greater than 300
                            latency_data[strategy].append(latency_value)
            except FileNotFoundError:
                print(f"File not found: {file_path}")
                continue
            except ValueError as e:
                print(f"Value error for file {file_path}: {e}")
                continue

            if len(latency_data[strategy]) == 0:
                print(f"No data read from file: {file_path}")
                continue

            avg_latencies[strategy] = np.mean(latency_data[strategy])

        # Determine the strategies with minimum and maximum average latency
        transnfv_strategy = min(["Partitioning", "Replication", "Offloading", "S6"], key=lambda x: avg_latencies.get(x, float('inf')))
        s6_strategy = max(["Replication", "S6"], key=lambda x: avg_latencies.get(x, float('inf')))

        # Define the mappings
        strategy_mapping = {
            "TransNFV": transnfv_strategy,
            "S6": s6_strategy,
            "OpenNF": "OpenNF",
            "CHC": "CHC"
        }

        # Collect the latency data for final strategies
        for final_strategy, mapped_strategy in strategy_mapping.items():
            if mapped_strategy in latency_data and len(latency_data[mapped_strategy]) > 0:
                final_latency_data[final_strategy].extend(latency_data[mapped_strategy])

    # Plot the CDF for each final strategy
    plt.figure(figsize=(7, 6))
    for strategy, latencies in final_latency_data.items():
        if len(latencies) == 0:
            continue

        latencies_sorted = np.sort(latencies)
        cdf = np.arange(1, len(latencies_sorted) + 1) / len(latencies_sorted)
        plt.plot(latencies_sorted, cdf, marker='.', linestyle='none', label=strategy)

#     plt.title("CDF of Latency for Final Strategies")
    plt.xlabel("Latency (1e-6 second)", fontsize=24)
    plt.ylabel("CDF", fontsize=24)
    plt.grid(True)
    plt.legend(loc='lower right', fontsize=24, handletextpad=0.2, markerscale=4)
    plt.xscale('log')  # Set x-axis to log scale
    plt.xticks(fontsize=20)  # Set y-axis number sizes to 14
    plt.yticks(fontsize=20)  # Set y-axis number sizes to 14

    plt.tight_layout()
    script_dir = os.path.dirname(__file__)  # Get the directory where the script is located
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, '5.2.2_Latency.pdf'))
    plt.savefig(os.path.join(figure_dir, '5.2.2_Latency.png'))
    print("5.2.2 latency figure generated.")



if __name__ == "__main__":
    # Define parameters
    app = "nfv_test"
    checkpointInterval = 100
    numTPGThreads = 8
    scheduler = "OP_BFS"
    NUM_ITEMS = 10000
    totalEvents = 400000
    nfvExperimentPath = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV"
    communicationChoice = 0
    numInstances = 4
    numOffloadThreads = 16
    offloadLockNum = 10000
    ccStrategy = 0
    workloadPattern = 0
    enableTimeBreakdown = 0
    instancePatternPunctuation = 25000
    expID = "5.2.2"
    enableHardcodeCCSwitch = 1
    script_path = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID

    generate_bash_script(app, checkpointInterval, numTPGThreads, scheduler, NUM_ITEMS, totalEvents, nfvExperimentPath,
                         communicationChoice, numInstances, numOffloadThreads, offloadLockNum, ccStrategy,
                         workloadPattern, enableTimeBreakdown, instancePatternPunctuation, expID, script_path,
                         enableHardcodeCCSwitch)
    execute_bash_script(script_path)

    plot_dynamic_throughput_linechart()
    plot_latency_CDF()

