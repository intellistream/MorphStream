import subprocess
import os
import time
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import csv

def generate_bash_script(app, checkpointInterval, tthread, scheduler, NUM_ITEMS, totalEvents, nfvWorkloadPath,
                         communicationChoice, vnfInstanceNum, offloadCCThreadNum, offloadLockNum, ccStrategy,
                         workloadPattern, enableTimeBreakdown, experimentID, script_path):
    script_content = f"""#!/bin/bash

function ResetParameters() {{
  app="{app}"
  checkpointInterval={checkpointInterval}
  tthread={tthread}
  scheduler="{scheduler}"
  NUM_ITEMS={NUM_ITEMS}
  totalEvents={totalEvents}
  nfvWorkloadPath="{nfvWorkloadPath}"
  communicationChoice={communicationChoice}
  vnfInstanceNum={vnfInstanceNum}
  offloadCCThreadNum={offloadCCThreadNum}
  offloadLockNum={offloadLockNum}
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
          --checkpoint_interval $checkpointInterval \\
          --totalEvents $totalEvents \\
          --nfvWorkloadPath $nfvWorkloadPath \\
          --communicationChoice $communicationChoice \\
          --vnfInstanceNum $vnfInstanceNum \\
          --offloadCCThreadNum $offloadCCThreadNum \\
          --offloadLockNum $offloadLockNum \\
          --ccStrategy $ccStrategy \\
          --workloadPattern $workloadPattern \\
          --enableTimeBreakdown $enableTimeBreakdown \\
          --experimentID $experimentID
          "
  java -Xms100g -Xmx100g -Xss10M -jar -d64 /home/shuhao/DB4NFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
    --app $app \\
    --NUM_ITEMS $NUM_ITEMS \\
    --tthread $tthread \\
    --scheduler $scheduler \\
    --checkpoint_interval $checkpointInterval \\
    --totalEvents $totalEvents \\
    --nfvWorkloadPath $nfvWorkloadPath \\
    --communicationChoice $communicationChoice \\
    --vnfInstanceNum $vnfInstanceNum \\
    --offloadCCThreadNum $offloadCCThreadNum \\
    --offloadLockNum $offloadLockNum \\
    --ccStrategy $ccStrategy \\
    --workloadPattern $workloadPattern \\
    --enableTimeBreakdown $enableTimeBreakdown \\
    --experimentID $experimentID
}}

function baselinePattern() {{
  ResetParameters
  for workloadPattern in 0 1 2 3
  do
    for ccStrategy in 0 1 2
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
    # Define the original and new pattern names and original CC strategy names
    original_patterns = ["loneOperative", "sharedReaders", "sharedWriters", "mutualInteractive"]
    new_patterns = ["Lone\nOperative", "Shared\nReaders", "Shared\nWriters", "Mutually\nInteractive"]
    original_cc_strategies = ["Partitioning", "Replication", "Offloading"]
    new_cc_strategies = ["Partitioning", "Replication", "Offloading"]
    colors = ['#2874A9', '#82E0AA', '#F1C70F']
    hatches = ['\\', '///', '\\\\']

    # Prepare the structure to hold data
    data = {pattern: {} for pattern in original_patterns}

    # Iterate over the patterns and ccStrategies
    for pattern in original_patterns:
        for strategy in original_cc_strategies:
            file_path = f"{root_directory}/{pattern}/{strategy}.csv"

            # Read the CSV file
            try:
                df = pd.read_csv(file_path, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                data[pattern][strategy] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {file_path}: {e}")
                data[pattern][strategy] = None

    # Plotting the data
    fig, ax = plt.subplots(figsize=(8, 5))  # Adjust the figure size as needed

    # Define the positions of the groups of bars
    bar_width = 0.2
    indices = list(range(len(original_patterns)))
    positions = [[i + j*bar_width for i in indices] for j in range(len(new_cc_strategies))]

    for i, (pattern, strategies) in enumerate(data.items()):
        throughputs = [strategies[orig_strat] for orig_strat in original_cc_strategies]

        for j, throughput in enumerate(throughputs):
            ax.bar(positions[j][i], throughput, color=colors[j], hatch=hatches[j], edgecolor="black", width=bar_width, label=new_cc_strategies[j] if i == 0 else "")

    # Set x-axis labels
    ax.set_xticks([r + bar_width for r in range(len(original_patterns))])
    ax.set_xticklabels(new_patterns, fontsize=16)
    ax.set_ylabel('Throughput (10^6 req/sec)', fontsize=18, labelpad=12)
    ax.set_xlabel('VNF State Access Patterns', fontsize=18, labelpad=12)

    # Adjust y-axis tick label font size
    ax.tick_params(axis='y', labelsize=14)

    # Create custom legend with hatches
    handles = [Patch(facecolor=color, edgecolor='black', hatch=hatch, label=label) for color, hatch, label in zip(colors, hatches, new_cc_strategies)]
    ax.legend(handles=handles, loc='upper right', ncol=1, fontsize=18)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    # Save the figure in the same directory as the script
    script_dir = os.path.dirname(__file__)  # Get the directory where the script is located
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, '5.1_Throughput.pdf'))  # Save the figure
    plt.savefig(os.path.join(figure_dir, '5.1_Throughput.png'))  # Save the figure


if __name__ == "__main__":
    # Define parameters
    app = "nfv_test"
    checkpointInterval = 500 # TPG batch size
    tthread = 8
    scheduler = "OP_BFS"
    NUM_ITEMS = 5000
    totalEvents = 400000
    nfvWorkloadPath = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV"
    communicationChoice = 0
    vnfInstanceNum = 4
    offloadCCThreadNum = 4
    offloadLockNum = 10000
    ccStrategy = 0
    workloadPattern = 0
    enableTimeBreakdown = 0
    experimentID = "5.1"
    script_path = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % experimentID

    generate_bash_script(app, checkpointInterval, tthread, scheduler, NUM_ITEMS, totalEvents, nfvWorkloadPath, communicationChoice,
                         vnfInstanceNum, offloadCCThreadNum, offloadLockNum, ccStrategy, workloadPattern, enableTimeBreakdown, experimentID, script_path)
    execute_bash_script(script_path)

    throughput_root_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.1/throughput"
    plot_throughput_barchart(throughput_root_directory)
    print("5.1 throughput figure generated.")
