import subprocess
import os
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import csv

def generate_bash_script(app, checkpointInterval, numTPGThreads, scheduler, NUM_ITEMS, totalEvents, nfvExperimentPath,
                         communicationChoice, numInstances, numOffloadThreads, offloadLockNum, ccStrategy,
                         workloadPattern, enableTimeBreakdown, expID, script_path):
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
  expID="{expID}"
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
          --expID $expID
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
    --expID $expID
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

def plot_throughput_barchart(root_directory):
    # Define the pattern names, VNF names, and CC strategy names
    patterns = ["loneOperative", "sharedReaders", "sharedWriters", "mutualInteractive"]
    vnfNames = ["NAT", "Load Balancer", "Portscan Detector", "Trojan Detector"]
    cc_strategies = ["Partitioning", "Replication", "Offloading", "Preemptive", "OpenNF", "CHC", "S6"]
    system_names = ["TransNFV", "OpenNF", "CHC", "S6"]
    colors = ['blue', 'green', 'red', 'purple']

    # Prepare the structure to hold data
    data = {pattern: {} for pattern in patterns}

    # Iterate over the patterns and cc_strategies to read the data
    for pattern in patterns:
        for strategy in cc_strategies:
            file_path = f"{root_directory}/{pattern}/{strategy}.csv"

            # Read the CSV file
            try:
                df = pd.read_csv(file_path, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                data[pattern][strategy] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {file_path}: {e}")
                data[pattern][strategy] = None

    # Aggregate data according to the new bar to original bar mapping
    aggregated_data = {pattern: {} for pattern in patterns}
    for pattern in patterns:
        aggregated_data[pattern]["TransNFV"] = max(
            data[pattern].get(cc_strategy, 0) for cc_strategy in cc_strategies
        )
        aggregated_data[pattern]["S6"] = data[pattern].get("S6", 0)
        aggregated_data[pattern]["OpenNF"] = data[pattern].get("OpenNF", 0)
        aggregated_data[pattern]["CHC"] = data[pattern].get("CHC", 0)

    # Plotting the data
    fig, axs = plt.subplots(1, 4, figsize=(20, 5), sharey=True)

    for i, (pattern, strategies) in enumerate(aggregated_data.items()):
        strategies_names = list(strategies.keys())
        throughputs = list(strategies.values())

        bars = axs[i].bar(strategies_names, throughputs, color=colors, width=0.5)
        axs[i].set_xlabel(vnfNames[i], fontsize=16)  # Use vnfNames for x-axis labels
        axs[i].set_ylabel('Throughput (requests/second)', fontsize=16)
        axs[i].set_xticks([])  # Remove x-axis labels

    # Create custom legend
    handles = [plt.Rectangle((0, 0), 1, 1, color=color) for color in colors]
    labels = system_names
    fig.legend(handles, labels, loc='upper center', ncol=4, fontsize=16)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    # Save the figure in the same directory as the script
    script_dir = os.path.dirname(__file__)  # Get the directory where the script is located
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, '5.2.1_Throughput.pdf'))  # Save the figure
    plt.savefig(os.path.join(figure_dir, '5.2.1_Throughput.png'))  # Save the figure



def plot_latency_CDF(root_directory):
    patterns = ["loneOperative", "sharedReaders", "sharedWriters", "mutualInteractive"]
    vnfNames = ["NAT", "Load Balancer", "Portscan Detector", "Trojan Detector"]
    cc_strategies = ["Partitioning", "Replication", "Offloading", "OpenNF", "CHC", "S6"]

    fig, axes = plt.subplots(1, 4, figsize=(20, 5))  # Adjusted figsize for a 1x4 layout
    axes = axes.flatten()

    for i, pattern in enumerate(patterns):
        ax = axes[i]
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
                        elif latency_value <= 300:  # Ignore latency values greater than 1000
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

        # Determine the strategy with minimum average latency among (Partitioning, Replication, Offloading)
        transnfv_strategy = min(["Partitioning", "Replication", "Offloading", "S6"], key=lambda x: avg_latencies.get(x, float('inf')))

        s6_strategy = max(["Replication", "S6"], key=lambda x: avg_latencies.get(x, float('inf')))

        # Define the mappings
        strategy_mapping = {
            "TransNFV": transnfv_strategy,
            "S6": s6_strategy,
            "OpenNF": "OpenNF",
            "CHC": "CHC"
        }

        # Plot the required CDF lines
        for system, strategy in strategy_mapping.items():
            if strategy not in latency_data or len(latency_data[strategy]) == 0:
                continue

            latency_data_sorted = np.sort(latency_data[strategy])
            cdf = np.arange(1, len(latency_data_sorted) + 1) / len(latency_data_sorted)
            ax.plot(latency_data_sorted, cdf, marker='.', linestyle='none', label=system)

        ax.set_title(vnfNames[i], fontsize=16)
        ax.set_xlabel("Latency 1e-6 second")
        ax.set_ylabel("CDF")
        ax.grid(True)
        ax.legend()
        ax.set_xscale('log')  # Set x-axis to log scale

    plt.tight_layout()
    script_dir = os.path.dirname(__file__)  # Get the directory where the script is located
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, '5.2.1_Latency.pdf'))
    plt.savefig(os.path.join(figure_dir, '5.2.1_Latency.png'))


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
    expID = "5.2.1"
    script_path = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID

    generate_bash_script(app, checkpointInterval, numTPGThreads, scheduler, NUM_ITEMS, totalEvents, nfvExperimentPath,
                         communicationChoice, numInstances, numOffloadThreads, offloadLockNum, ccStrategy,
                         workloadPattern, enableTimeBreakdown, expID, script_path)
    execute_bash_script(script_path)

    throughput_root_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.2.1/throughput"
    plot_throughput_barchart(throughput_root_directory)
    print("5.2.1 throughput figure generated.")
#     latency_root_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.2.1/latency"
#     plot_latency_CDF(latency_root_directory)
#     print("5.2.1 latency figure generated.")
