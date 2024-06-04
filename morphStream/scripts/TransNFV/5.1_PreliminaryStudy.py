import subprocess
import os
import time
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import csv

def generate_bash_script(app, checkpointInterval, tthread, scheduler, defaultScheduler, complexity, NUM_ITEMS, rootFilePath, totalEvents, nfvWorkloadPath, communicationChoice, vnfInstanceNum, offloadCCThreadNum, offloadLockNum, rRatioSharedReaders, wRatioSharedWriters, rwRatioMutualInteractive, ccStrategy, workloadPattern, enableTimeBreakdown, experimentID, script_path):
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
  enableTimeBreakdown={enableTimeBreakdown}
  experimentID="{experimentID}"
}}

function runTStream() {{
  echo "java -Xms100g -Xmx100g -jar -d64 /home/shuhao/DB4NFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
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
          --enableTimeBreakdown $enableTimeBreakdown \\
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
    --enableTimeBreakdown $enableTimeBreakdown \\
    --experimentID $experimentID
}}

function baselinePattern() {{
  ResetParameters
  for workloadPattern in 0 1 2 3
  do
    for ccStrategy in 0 1 2 3
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
    cc_strategies = ["Partitioning", "Replication", "Offloading", "Preemptive"]
    colors = ['blue', 'green', 'red', 'purple']

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

    for i, (pattern, strategies) in enumerate(data.items()):
        strategies_names = list(strategies.keys())
        throughputs = list(strategies.values())

        bars = axs[i].bar(strategies_names, throughputs, color=colors, width=0.5)
        # axs[i].set_title(f'Throughput for {pattern}')
        axs[i].set_xlabel(pattern, fontsize=16)
        axs[i].set_ylabel('Throughput (requests/second)', fontsize=16)
        axs[i].set_xticks([])  # Remove x-axis labels

    # Create custom legend
    handles = [plt.Rectangle((0,0),1,1, color=color) for color in colors]
    labels = cc_strategies
    fig.legend(handles, labels, loc='upper center', ncol=4, fontsize=16)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    # Save the figure in the same directory as the script
    script_dir = os.path.dirname(__file__)  # Get the directory where the script is located
    plt.savefig(os.path.join(script_dir, '5.1_Throughput.pdf'))  # Save the figure
    plt.savefig(os.path.join(script_dir, '5.1_Throughput.png'))  # Save the figure


if __name__ == "__main__":
    # Define parameters
    app = "nfv_test"
    checkpointInterval = 500
    tthread = 8
    scheduler = "OP_BFS_A"
    defaultScheduler = "OP_BFS_A"
    complexity = 0
    NUM_ITEMS = 10000
    rootFilePath = "/home/shuhao/jjzhao/data"
    totalEvents = 400000
    nfvWorkloadPath = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV"
    communicationChoice = 0
    vnfInstanceNum = 4
    offloadCCThreadNum = 16
    offloadLockNum = 10000
    rRatioSharedReaders = 80
    wRatioSharedWriters = 80
    rwRatioMutualInteractive = 80
    ccStrategy = 0
    workloadPattern = 0
    enableTimeBreakdown = 0
    experimentID = "5.1"
    script_path = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/%s.sh" % experimentID

    generate_bash_script(app, checkpointInterval, tthread, scheduler, defaultScheduler, complexity, NUM_ITEMS, rootFilePath, totalEvents, nfvWorkloadPath, communicationChoice, vnfInstanceNum, offloadCCThreadNum, offloadLockNum, rRatioSharedReaders, wRatioSharedWriters, rwRatioMutualInteractive, ccStrategy, workloadPattern, enableTimeBreakdown, experimentID, script_path)
    execute_bash_script(script_path)

    throughput_root_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.1/throughput"
    plot_throughput_barchart(throughput_root_directory)
    print("5.1 throughput figure generated.")
