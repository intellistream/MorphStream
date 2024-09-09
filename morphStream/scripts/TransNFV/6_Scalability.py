import subprocess
import os
import time
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
          --defaultScheduler $defaultScheduler \\
          --checkpoint_interval $checkpointInterval \\
          --complexity $complexity \\
          --rootFilePath $rootFilePath \\
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
    --defaultScheduler $defaultScheduler \\
    --checkpoint_interval $checkpointInterval \\
    --complexity $complexity \\
    --rootFilePath $rootFilePath \\
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
    for ccStrategy in 0 1 2 3 4 5
    do
      for numInstances in 4 8 16
      do
        totalEvents=320000
      runTStream
    done
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


def plot_scalability_comparison(root_dir):
    # Define patterns, strategies, and number of parallel instances
    patterns = ["loneOperative", "sharedReaders", "sharedWriters", "mutualInteractive"]
    strategies = ["Partitioning", "Replication", "Offloading", "Preemptive", "Broadcasting", "Flushing"]
    parallel_instances = [4, 8, 12]

    # Function to read throughput data from CSV
    def read_throughput_data(pattern, strategy, instance_number):
        file_path = f"{root_dir}/numInstance_{instance_number}/{pattern}/{strategy}.csv"
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            row = next(reader)
            throughput = float(row[2])  # Assuming the throughput is the third element
        return throughput

    # Initialize a dictionary to store the throughput data
    throughput_data = {pattern: {strategy: [] for strategy in strategies} for pattern in patterns}

    # Read throughput data from CSV files
    for pattern in patterns:
        for strategy in strategies:
            for instance_number in parallel_instances:
                throughput = read_throughput_data(pattern, strategy, instance_number)
                throughput_data[pattern][strategy].append(throughput)

    # Create subplots
    fig, axs = plt.subplots(1, 4, figsize=(20, 5))

    # Flatten axs array for easy iteration
    axs = axs.flatten()

    # Plot each pattern in a separate subplot
    for i, pattern in enumerate(patterns):
        ax = axs[i]
        for strategy in strategies:
            ax.plot(parallel_instances, throughput_data[pattern][strategy], marker='o', linestyle='-', label=strategy)
        ax.set_title(f'{pattern} Workload')
        ax.set_xlabel('Number of Parallel Instances')
        ax.set_ylabel('Throughput')
        ax.set_xticks(parallel_instances)  # Set x-ticks to only show actual data points
        ax.set_xticklabels(parallel_instances)  # Explicitly set the tick labels
        ax.grid(True)
        ax.legend()

    # Adjust layout
    plt.tight_layout()

    # Adjust layout
    plt.tight_layout()
    script_dir = os.path.dirname(__file__)  # Get the directory where the script is located
    figure_dir = os.path.join(script_dir, "figures")
    plt.savefig(os.path.join(figure_dir, '5.2.2_Scalability.png'))
    plt.savefig(os.path.join(figure_dir, '5.2.2_Scalability.pdf'))
    print("VNF scalability figure generated.")


if __name__ == "__main__":
    # Define parameters
    app = "nfv_test"
    checkpointInterval = 100
    numTPGThreads = 4
    scheduler = "OP_BFS"
    NUM_ITEMS = 10000
    totalEvents = 4000
    nfvExperimentPath = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV"
    communicationChoice = 0
    numInstances = 4
    numOffloadThreads = 4
    offloadLockNum = 1000
    ccStrategy = 0
    workloadPattern = 0
    enableTimeBreakdown = 0
    expID = "6"
    script_path = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/%s.sh" % expID

    generate_bash_script(app, checkpointInterval, numTPGThreads, scheduler, NUM_ITEMS, totalEvents, nfvExperimentPath,
                         communicationChoice, numInstances, numOffloadThreads, offloadLockNum, ccStrategy,
                         workloadPattern, enableTimeBreakdown, expID, script_path)
    execute_bash_script(script_path)

    throughput_root_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/6/throughput"
    plot_scalability_comparison(throughput_root_directory)
