import subprocess
import os
import time
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import csv

def generate_bash_script(app, checkpointInterval, tthread, scheduler, defaultScheduler, complexity, NUM_ITEMS, rootFilePath,
                         totalEvents, nfvWorkloadPath, communicationChoice, vnfInstanceNum, offloadCCThreadNum, offloadLockNum,
                         ccStrategy, workloadPattern, enableTimeBreakdown,
                         experimentID, script_path, enableHardcodeCCSwitch, enableMemoryFootprint, memoryIntervalMS,
                         instancePatternPunctuation, managerPatternPunctuation):
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
  ccStrategy={ccStrategy}
  workloadPattern={workloadPattern}
  enableTimeBreakdown={enableTimeBreakdown}
  experimentID="{experimentID}"
  enableHardcodeCCSwitch="{enableHardcodeCCSwitch}"
  instancePatternPunctuation={instancePatternPunctuation}
  managerPatternPunctuation={managerPatternPunctuation}
  enableMemoryFootprint={enableMemoryFootprint}
  memoryIntervalMS={memoryIntervalMS}
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
          --ccStrategy $ccStrategy \\
          --workloadPattern $workloadPattern \\
          --enableTimeBreakdown $enableTimeBreakdown \\
          --experimentID $experimentID \\
          --enableHardcodeCCSwitch $enableHardcodeCCSwitch \\
          --instancePatternPunctuation $instancePatternPunctuation \\
          --managerPatternPunctuation $managerPatternPunctuation \\
          --enableMemoryFootprint $enableMemoryFootprint \\
          --memoryIntervalMS $memoryIntervalMS
          "
  java -Xms100g -Xmx100g -Xss10M -jar -d64 /home/shuhao/DB4NFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
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
    --ccStrategy $ccStrategy \\
    --workloadPattern $workloadPattern \\
    --enableTimeBreakdown $enableTimeBreakdown \\
    --experimentID $experimentID \\
    --enableHardcodeCCSwitch $enableHardcodeCCSwitch \\
    --instancePatternPunctuation $instancePatternPunctuation \\
    --managerPatternPunctuation $managerPatternPunctuation \\
    --enableMemoryFootprint $enableMemoryFootprint \\
    --memoryIntervalMS $memoryIntervalMS
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

def read_time(path, system, index):
    directory = os.path.join("/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.3.1/", f"{path}/")
    file_path = os.path.join(directory, f"{system}.csv")
    time = -1
    try:
        with open(file_path, 'r') as file:
            for line_num, line in enumerate(file):
                values = line.split(',')
                if index < len(values):
                    time = float(values[index])
                    return time
                else:
                    print(f"Index {index} out of range for line: {line}")
                    return None
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
    return time

def compute_intervals(strategy):
    chc_start_time = read_time("start_times", strategy, 0)
    chc_init_time = read_time("timestamps", strategy, 0)
    chc_process_time = read_time("timestamps", strategy, 1)
    chc_init_interval = int((chc_init_time - chc_start_time) / 10000000)
    chc_process_interval = int((chc_process_time - chc_start_time) / 10000000)
    print(chc_process_time - chc_init_time)
    print(chc_init_interval)
    print(chc_process_interval)
    return (chc_init_interval, chc_process_interval)

def draw_footprint_plot():
    footprint_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.3.1/memory_footprint/"
    chc_init_interval, chc_process_interval = compute_intervals("CHC")
    s6_init_interval, s6_process_interval = compute_intervals("S6")
    openNF_init_interval, openNF_process_interval = compute_intervals("OpenNF")

    transNFV_start_time = read_time("start_times", "TransNFV", 0)

    transNFV_init_time = -1
    transNFV_process_time = -1
    strategies = ["Partitioning", "Replication", "Offloading"]
    for strategy in strategies:
        init_time = read_time("timestamps", strategy, 0)
        process_time = read_time("timestamps", strategy, 1)
        if init_time > transNFV_init_time:
            transNFV_init_time = init_time
        if process_time > transNFV_process_time:
            transNFV_process_time = process_time

    first_elements = []
    second_elements = []

    tpg_timestamp_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.3.1/timestamps/Preemptive"
    for filename in os.listdir(tpg_timestamp_directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(tpg_timestamp_directory, filename)
            try:
                with open(file_path, 'r') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        if len(row) >= 2:  # Ensure there are at least two elements in the row
                            first_elements.append(float(row[0]))
                            second_elements.append(float(row[1]))
            except FileNotFoundError:
                print(f"File not found: {file_path}")
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")

    # Calculate the max values
    max_first_element = max(first_elements) if first_elements else None
    max_second_element = max(second_elements) if second_elements else None
    if max_first_element > transNFV_init_time:
        transNFV_init_time = max_first_element
    if max_second_element > transNFV_process_time:
        transNFV_process_time = max_second_element

    timestamp_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.3.1/timestamps/"
    strategy_names = ["OpenNF", "S6", "CHC", "TransNFV"]

    fig, ax = plt.subplots(figsize=(5.5, 5))

    # Markers and colors for each strategy
    markers = ['o', 's', '^', 'd']
    colors = ['b', 'r', 'g', 'm']
    for strategy, marker, color in zip(strategy_names, markers, colors):
       file_path = os.path.join(footprint_directory, f"{strategy}.csv")

       # Read data from CSV file
       x_values = []
       y_values = []
       try:
           with open(file_path, 'r') as file:
               for index, line in enumerate(file):
                   y_value = float(line.split(',')[1])  # Read the first element as the y-value
                   y_values.append(y_value)
               x_values = range(len(y_values))  # Use the index of each line as the x-value

           # Plotting the line without markers
           ax.plot(x_values, y_values, color=color, label=strategy)
           # Adding markers every 10 points
           ax.plot(x_values[::10], y_values[::10], marker=marker, linestyle='none', color=color, label="_nolegend_")

       except FileNotFoundError:
           print(f"File not found: {file_path}")
       except Exception as e:
           print(f"Error reading file {file_path}: {e}")

    ax.legend(loc='lower right', fontsize=18)
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    ax.set_xlabel('Time (10^-2 seconds)', fontsize=18)
    ax.set_ylabel('Memory Usage (bytes)', fontsize=18)

    plt.tight_layout()
    script_dir = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/"
    plt.savefig(os.path.join(script_dir, '5.3.2_Footprint.pdf'))
    plt.savefig(os.path.join(script_dir, '5.3.2_Footprint.png'))
    print("Figure generated.")


if __name__ == "__main__":
    # Define parameters
    app = "nfv_test"
    checkpointInterval = 100
    tthread = 8
    scheduler = "OP_BFS_A"
    defaultScheduler = "OP_BFS_A"
    complexity = 0
    NUM_ITEMS = 10000
    rootFilePath = "/home/shuhao/jjzhao/data"
    totalEvents = 1200000
    nfvWorkloadPath = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV"
    communicationChoice = 0
    vnfInstanceNum = 4
    offloadCCThreadNum = 16
    offloadLockNum = 10000
    ccStrategy = 0
    workloadPattern = 0
    enableTimeBreakdown = 1
    experimentID = "5.3.1"
    enableHardcodeCCSwitch = 1
    enableMemoryFootprint = 1
    memoryIntervalMS = 10
    instancePatternPunctuation = 25000
    managerPatternPunctuation = 100000
    script_path = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/%s.sh" % experimentID

#     generate_bash_script(app, checkpointInterval, tthread, scheduler, defaultScheduler, complexity, NUM_ITEMS, rootFilePath, totalEvents, nfvWorkloadPath, communicationChoice, vnfInstanceNum, offloadCCThreadNum, offloadLockNum, ccStrategy, workloadPattern, enableTimeBreakdown, experimentID, script_path, enableHardcodeCCSwitch, enableMemoryFootprint, memoryIntervalMS, instancePatternPunctuation, managerPatternPunctuation)
#     execute_bash_script(script_path)

    draw_footprint_plot()


