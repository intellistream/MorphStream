import subprocess
import os
import time
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import csv

def generate_bash_script(app, checkpointInterval, tthread, scheduler, NUM_ITEMS, totalEvents, nfvWorkloadPath,
                         communicationChoice, vnfInstanceNum, offloadCCThreadNum, offloadLockNum,
                         ccStrategy, workloadPattern, enableTimeBreakdown, experimentID, script_path,
                         enableHardcodeCCSwitch, instancePatternPunctuation, managerPatternPunctuation):
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
  enableHardcodeCCSwitch="{enableHardcodeCCSwitch}"
  instancePatternPunctuation={instancePatternPunctuation}
  managerPatternPunctuation={managerPatternPunctuation}
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
          --experimentID $experimentID \\
          --enableHardcodeCCSwitch $enableHardcodeCCSwitch \\
          --instancePatternPunctuation $instancePatternPunctuation \\
          --managerPatternPunctuation $managerPatternPunctuation
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
    --experimentID $experimentID \\
    --enableHardcodeCCSwitch $enableHardcodeCCSwitch \\
    --instancePatternPunctuation $instancePatternPunctuation \\
    --managerPatternPunctuation $managerPatternPunctuation
}}

function baselinePattern() {{
  ResetParameters
  for workloadPattern in 4
  do
    for ccStrategy in 4 5 6 7
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


def plot_time_breakdown_barchart():
    base_path = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.3.1/breakdown/numInstance_4/dynamic/"
    systems = ["OpenNF", "CHC", "S6", "TransNFV"]
    categories = ['Parsing', 'Sync', 'Useful', 'CC Switch']
    hatches = ['\\', '//', '\\\\', '///']

    # Read data from CSV files
    sub_bar_values = {}
    for system in systems:
        file_path = f"{base_path}{system}.csv"
        data = pd.read_csv(file_path, header=None).values.flatten().tolist()
        sub_bar_values[system] = data

    # Data preparation
    systems = list(sub_bar_values.keys())
    values = np.array(list(sub_bar_values.values()))

    # Generate the figure
    fig, ax = plt.subplots(figsize=(7, 5))  # Adjusted size

    # Calculate the bottom values for stacking
    cumulative_values = np.zeros(len(systems))
    bar_height = 0.4  # Reduced bar width

    for i in range(values.shape[1]):
        bars = ax.barh(systems, values[:, i], left=cumulative_values, height=bar_height, label=categories[i], hatch=hatches[i % len(hatches)])
        cumulative_values += values[:, i]

    # Add labels and title
    ax.set_xlabel('Execution Time (ms)', fontsize=20)
    ax.legend(loc='upper right', fontsize=20)
    plt.xticks(fontsize=18)  # Set y-axis number sizes to 14
    plt.yticks(fontsize=18)  # Set y-axis number sizes to 14
    ax.set_yticklabels(systems, rotation=50, fontsize=20)

    # Show the plot
    plt.tight_layout()
    script_dir = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/"
    figure_dir = os.path.join(script_dir, "figures")
    plt.savefig(os.path.join(figure_dir, '5.3.1_Overhead.pdf'))
    plt.savefig(os.path.join(figure_dir, '5.3.1_Overhead.png'))
    print("Figure generated.")



if __name__ == "__main__":
    # Define parameters
    app = "nfv_test"
    checkpointInterval = 100
    tthread = 8
    scheduler = "OP_BFS"
    NUM_ITEMS = 10000
    totalEvents = 1200000
    nfvWorkloadPath = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV"
    communicationChoice = 0
    vnfInstanceNum = 4
    offloadCCThreadNum = 16
    offloadLockNum = 10000
    ccStrategy = 0
    workloadPattern = 4 # Dynamic workload
    enableTimeBreakdown = 1
    experimentID = "5.3.1"
    enableHardcodeCCSwitch = 1
    instancePatternPunctuation = 25000
    managerPatternPunctuation = 100000
    script_path = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/%s.sh" % experimentID

    generate_bash_script(app, checkpointInterval, tthread, scheduler, NUM_ITEMS, totalEvents, nfvWorkloadPath,
                         communicationChoice, vnfInstanceNum, offloadCCThreadNum, offloadLockNum, ccStrategy,
                         workloadPattern, enableTimeBreakdown, experimentID, script_path, enableHardcodeCCSwitch,
                         instancePatternPunctuation, managerPatternPunctuation)
    execute_bash_script(script_path)

    plot_time_breakdown_barchart()

