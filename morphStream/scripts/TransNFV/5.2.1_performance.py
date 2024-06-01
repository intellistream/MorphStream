import subprocess
import os
import time
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import csv

def generate_bash_script(app, checkpointInterval, tthread, scheduler, defaultScheduler, complexity, NUM_ITEMS, rootFilePath, totalEvents, nfvWorkloadPath, communicationChoice, vnfInstanceNum, offloadCCThreadNum, offloadLockNum, rRatioSharedReaders, wRatioSharedWriters, rwRatioMutualInteractive, ccStrategy, workloadPattern, enableCCSwitch, experimentID, script_path):
    script_content = f"""#!/bin/bash

function ResetParameters() {{
  app="{app}"
  checkpointInterval={checkpointInterval}
  tthread={tthread}
  scheduler="{scheduler}"
  defaultScheduler="{defaultScheduler}"
  complexity={complexity}
  NUM_ITEMS={NUM_ITEMS}
  rootFilePath="{rootFilePath}"
  totalEvents={totalEvents}

  nfvWorkloadPath="{nfvWorkloadPath}"
  communicationChoice={communicationChoice}
  vnfInstanceNum={vnfInstanceNum}
  offloadCCThreadNum={offloadCCThreadNum}
  offloadLockNum={offloadLockNum}
  rRatioSharedReaders={rRatioSharedReaders}
  wRatioSharedWriters={wRatioSharedWriters}
  rwRatioMutualInteractive={rwRatioMutualInteractive}
  ccStrategy={ccStrategy}
  workloadPattern={workloadPattern}
  enableCCSwitch={enableCCSwitch}
  experimentID="{experimentID}"
}}

function runTStream() {{
  echo "java -Xms20g -Xmx80g -jar -d64 /home/shuhao/DB4NFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
          --app $app \\
          --NUM_ITEMS $NUM_ITEMS \\
          --tthread $tthread \\
          --scheduler $scheduler \\
          --defaultScheduler $defaultScheduler \\
          --checkpoint_interval $checkpointInterval \\
          --complexity $complexity \\
          --rootFilePath $rootFilePath \\
          --totalEvents $totalEvents \\
          --nfvWorkloadPath $nfvWorkloadPath \\
          --communicationChoice $communicationChoice \\
          --vnfInstanceNum $vnfInstanceNum \\
          --offloadCCThreadNum $offloadCCThreadNum \\
          --offloadLockNum $offloadLockNum \\
          --rRatioSharedReaders $rRatioSharedReaders \\
          --wRatioSharedWriters $wRatioSharedWriters \\
          --rwRatioMutualInteractive $rwRatioMutualInteractive \\
          --ccStrategy $ccStrategy \\
          --workloadPattern $workloadPattern \\
          --enableCCSwitch $enableCCSwitch \\
          --experimentID $experimentID
          "
  java -Xms20g -Xmx80g -Xss10M -jar -d64 /home/shuhao/DB4NFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
    --app $app \\
    --NUM_ITEMS $NUM_ITEMS \\
    --tthread $tthread \\
    --scheduler $scheduler \\
    --defaultScheduler $defaultScheduler \\
    --checkpoint_interval $checkpointInterval \\
    --complexity $complexity \\
    --rootFilePath $rootFilePath \\
    --totalEvents $totalEvents \\
    --nfvWorkloadPath $nfvWorkloadPath \\
    --communicationChoice $communicationChoice \\
    --vnfInstanceNum $vnfInstanceNum \\
    --offloadCCThreadNum $offloadCCThreadNum \\
    --offloadLockNum $offloadLockNum \\
    --rRatioSharedReaders $rRatioSharedReaders \\
    --wRatioSharedWriters $wRatioSharedWriters \\
    --rwRatioMutualInteractive $rwRatioMutualInteractive \\
    --ccStrategy $ccStrategy \\
    --workloadPattern $workloadPattern \\
    --enableCCSwitch $enableCCSwitch \\
    --experimentID $experimentID
}}

function baselinePattern() {{
  ResetParameters
  for workloadPattern in 0 1 2 3
  do
    for ccStrategy in 0 1 2 3 4 5
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
    # Define the pattern names and CC strategy names
    patterns = ["loneOperative", "sharedReaders", "sharedWriters", "mutualInteractive"]
    cc_strategies = ["Partitioning", "Replication", "Offloading", "Preemptive", "Broadcasting", "Flushing"]

    # Prepare the structure to hold data
    data = {pattern: {} for pattern in patterns}

    # Iterate over the patterns and ccStrategies
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

    # Plotting the data
    fig, axs = plt.subplots(1, 4, figsize=(20, 5), sharey=True)
    fig.suptitle('Throughput Comparison Across Different CC Strategies for Each Pattern')

    for i, (pattern, strategies) in enumerate(data.items()):
        strategies_names = list(strategies.keys())
        throughputs = list(strategies.values())

        axs[i].bar(strategies_names, throughputs, color='blue')
        axs[i].set_title(f'Throughput for {pattern}')
        axs[i].set_xlabel('CC Strategy')
        axs[i].set_ylabel('Throughput (requests/second)')

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    # Save the figure in the same directory as the script
    script_dir = os.path.dirname(__file__)  # Get the directory where the script is located
    plt.savefig(os.path.join(script_dir, '5.2.1_Throughput.png'))  # Save the figure



def plot_latency_CDF(root_directory):
    patterns = ["loneOperative", "sharedReaders", "sharedWriters", "mutualInteractive"]
    strategies = ["Partitioning", "Replication", "Offloading", "Preemptive", "Broadcasting", "Flushing"]

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    axes = axes.flatten()

    for i, pattern in enumerate(patterns):
        ax = axes[i]
        for strategy in strategies:
            file_path = os.path.join(root_directory, pattern, f"{strategy}.csv")
            latency_data = []
            try:
                with open(file_path, 'r') as file:
                    reader = csv.reader(file)
                    next(reader)  # Skip the header
                    for row in reader:
                        latency_value = float(row[0])
                        if latency_value < 0:
                            print(f"Negative latency value found: {latency_value} in file {file_path}")
                        latency_data.append(latency_value)
            except FileNotFoundError:
                print(f"File not found: {file_path}")
                continue
            except ValueError as e:
                print(f"Value error for file {file_path}: {e}")
                continue

            if len(latency_data) == 0:
                print(f"No data read from file: {file_path}")
                continue

            latency_data_sorted = np.sort(latency_data)
            cdf = np.arange(1, len(latency_data_sorted) + 1) / len(latency_data_sorted)

            if np.any(latency_data_sorted < 0):
                print(f"Negative values in sorted data from file {file_path}")

            if np.any(cdf < 0):
                print(f"Negative values in CDF from file {file_path}")

            ax.plot(latency_data_sorted, cdf, marker='.', linestyle='none', label=strategy)

        ax.set_title(f"Pattern {pattern}")
        ax.set_xlabel("Latency (10^-6 seconds)")
        ax.set_ylabel("CDF")
        ax.grid(True)
        ax.legend()

    plt.tight_layout()
    script_dir = os.path.dirname(__file__)  # Get the directory where the script is located
    plt.savefig(os.path.join(script_dir, '5.2.1_Latency.png'))


if __name__ == "__main__":
    # Define parameters
    app = "nfv_test"
    checkpointInterval = 100
    tthread = 4
    scheduler = "OP_BFS_A"
    defaultScheduler = "OP_BFS_A"
    complexity = 0
    NUM_ITEMS = 10000
    rootFilePath = "/home/shuhao/jjzhao/data"
    totalEvents = 4000
    nfvWorkloadPath = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV"
    communicationChoice = 0
    vnfInstanceNum = 4
    offloadCCThreadNum = 4
    offloadLockNum = 1000
    rRatioSharedReaders = 80
    wRatioSharedWriters = 80
    rwRatioMutualInteractive = 80
    ccStrategy = 0
    workloadPattern = 0
    enableCCSwitch = 0
    experimentID = "5.2.1"
    script_path = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/%s.sh" % experimentID

    # generate_bash_script(app, checkpointInterval, tthread, scheduler, defaultScheduler, complexity, NUM_ITEMS, rootFilePath, totalEvents, nfvWorkloadPath, communicationChoice, vnfInstanceNum, offloadCCThreadNum, offloadLockNum, rRatioSharedReaders, wRatioSharedWriters, rwRatioMutualInteractive, ccStrategy, workloadPattern, enableCCSwitch, experimentID, script_path)
    # execute_bash_script(script_path)

    throughput_root_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.2.1/throughput"
    plot_throughput_barchart(throughput_root_directory)
    latency_root_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.2.1/latency"
    plot_latency_CDF(latency_root_directory)
