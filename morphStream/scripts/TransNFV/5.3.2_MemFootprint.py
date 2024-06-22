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
                         enableHardcodeCCSwitch, enableMemoryFootprint, memoryIntervalMS,
                         instancePatternPunctuation, managerPatternPunctuation):
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
  enableMemoryFootprint={enableMemoryFootprint}
  memoryIntervalMS={memoryIntervalMS}
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
          --managerPatternPunctuation $managerPatternPunctuation \\
          --enableMemoryFootprint $enableMemoryFootprint \\
          --memoryIntervalMS $memoryIntervalMS
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
    directory = os.path.join("/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.3.2/", f"{path}/")
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
    chc_init_interval = int((chc_init_time - chc_start_time) / 10000000) + 1
    chc_process_interval = int((chc_process_time - chc_start_time) / 10000000) + 1
    print(chc_init_interval)
    print(chc_process_interval)
    return (chc_init_interval, chc_process_interval)

def compute_intervals_transNFV():
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

#     print(transNFV_init_time)
#     print(transNFV_process_time)

    init_times = []
    process_times = []
    tpg_timestamp_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.3.2/timestamps/Preemptive"
    for filename in os.listdir(tpg_timestamp_directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(tpg_timestamp_directory, filename)
            try:
                with open(file_path, 'r') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        if len(row) >= 2:  # Ensure there are at least two elements in the row
                            init_times.append(float(row[0]))
                            process_times.append(float(row[1]))
            except FileNotFoundError:
                print(f"File not found: {file_path}")
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")
    max_init_time = min(init_times) if init_times else None
    max_process_time = max(process_times) if process_times else None
    if max_init_time > transNFV_init_time:
        transNFV_init_time = max_init_time
    if max_process_time > transNFV_process_time:
        transNFV_process_time = max_process_time
#     print(transNFV_init_time)
#     print(transNFV_process_time)

    transNFV_init_interval = int((transNFV_init_time - transNFV_start_time) / 10000000) + 1
    transNFV_process_interval = int((transNFV_process_time - transNFV_start_time) / 10000000) + 1
#     print(transNFV_init_interval)
#     print(transNFV_process_interval)
    return (transNFV_init_interval, transNFV_process_interval)

def draw_footprint_plot():
    footprint_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/results/5.3.2/memory_footprint/"
    chc_init_interval, chc_process_interval = compute_intervals("CHC")
    s6_init_interval, s6_process_interval = compute_intervals("S6")
    openNF_init_interval, openNF_process_interval = compute_intervals("OpenNF")
    transNFV_init_interval = 102
    transNFV_process_interval = 285

    init_intervals = [openNF_init_interval, s6_init_interval, chc_init_interval, transNFV_init_interval]
    process_intervals = [openNF_process_interval, s6_process_interval, chc_process_interval, transNFV_process_interval]
    strategy_names = ["OpenNF", "S6", "CHC", "TransNFV"]
    fig, ax = plt.subplots(figsize=(7,5))
    markers = ['o', 's', '^', 'd']
    colors = ['b', 'r', 'g', 'm']
    init_legend_handles = [plt.Line2D([0], [0], color='black', linestyle='--', linewidth=2, label='Initialization')]
    process_legend_handles = [plt.Line2D([0], [0], color='black', linestyle='-', linewidth=2, label='Processing')]

    for i, (strategy, marker, color) in enumerate(zip(strategy_names, markers, colors)):
        file_path = os.path.join(footprint_directory, f"{strategy}.csv")
        y_values = []
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    y_value = float(line.split(',')[1])  # Read the second element as the y-value
                    y_values.append(y_value)
            x_values = range(len(y_values))  # Use the index of each line as the x-value

            ax.plot(x_values, y_values, color=color, label=strategy)
            ax.plot(x_values[::20], y_values[::20], marker=marker, markersize=10, linestyle='none', color=color, label="_nolegend_")

            # Draw dashed lines for init and process intervals with different dash styles
            init_x = init_intervals[i]
            process_x = process_intervals[i]
            init_y = y_values[init_x] if init_x < len(y_values) else np.nan
            process_y = y_values[process_x] if process_x < len(y_values) else np.nan

            ax.axvline(x=init_x, color=color, linestyle='--', linewidth=2, alpha=0.7, label=f"{strategy} Init" if i == 0 else "")
            ax.axvline(x=process_x, color=color, linestyle='-', linewidth=2, alpha=0.7, label=f"{strategy} Process" if i == 0 else "")
            ax.plot([init_x, process_x], [init_y, process_y], color=color, linestyle='--', alpha=0.7)

        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")

    # Custom legend to include both lines and markers
    handles, labels = ax.get_legend_handles_labels()
    custom_handles = [plt.Line2D([0], [0], color=color, marker=marker, linestyle='-') for color, marker in zip(colors, markers)]
    legend_labels = strategy_names

    # Place legend outside the plot with 4 columns in one row
    top_legend = plt.legend(custom_handles, legend_labels, loc='upper right', fontsize=16, markerscale=1.5, handletextpad=0.1)
    plt.gca().add_artist(top_legend)
#     bottom = plt.legend(custom_handles, legend_labels, loc='lower center', bbox_to_anchor=(0.45, 1.2), ncol=4, fontsize=18, markerscale=1.5, handletextpad=0.5, columnspacing=0.5)
    plt.legend(handles=init_legend_handles + process_legend_handles,
               labels=['Initialization finish', 'Processing finish'], loc='lower right', fontsize=16)

    plt.xticks(fontsize=18)
    plt.yticks(fontsize=18)
    ax.set_xlabel('Time (10^-2 seconds)', fontsize=20, labelpad=10)
    ax.set_ylabel('Memory Usage (10^10 bytes)', fontsize=20, labelpad=10)

    plt.tight_layout()
    script_dir = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/"
    figure_dir = os.path.join(script_dir, "figures")
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, '5.3.2_Footprint.pdf'))
    plt.savefig(os.path.join(figure_dir, '5.3.2_Footprint.png'))
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
    enableTimeBreakdown = 0
    experimentID = "5.3.2"
    enableHardcodeCCSwitch = 1
    enableMemoryFootprint = 1
    memoryIntervalMS = 10
    instancePatternPunctuation = 25000
    managerPatternPunctuation = 100000
    script_path = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % experimentID

    generate_bash_script(app, checkpointInterval, tthread, scheduler, NUM_ITEMS, totalEvents, nfvWorkloadPath,
                         communicationChoice, vnfInstanceNum, offloadCCThreadNum, offloadLockNum, ccStrategy,
                         workloadPattern, enableTimeBreakdown, experimentID, script_path, enableHardcodeCCSwitch,
                         enableMemoryFootprint, memoryIntervalMS, instancePatternPunctuation, managerPatternPunctuation)
    execute_bash_script(script_path)

    draw_footprint_plot()


