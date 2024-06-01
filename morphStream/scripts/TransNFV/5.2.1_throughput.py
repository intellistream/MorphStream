import subprocess
import os
import time
import threading
import pandas as pd
import matplotlib.pyplot as plt

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

def execute_bash_script(script_path, vnf_finished_indicator):
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

    # Check for the specified file to be created
    while not os.path.exists(vnf_finished_indicator):
        print(f"Waiting for {vnf_finished_indicator} to be created...")
        time.sleep(1)

    if process.returncode != 0:
        print(f"Bash script finished with errors.")
    else:
        print(f"Bash script completed successfully.")

def read_and_plot(root_directory):
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
    plt.savefig(os.path.join(script_dir, 'throughput_comparison_figure.png'))  # Save the figure

    plt.show()  # Show the figure

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
    experimentID = "5.2.1_throughput"
    script_path = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/%s.sh" % experimentID
    vnf_finished_indicator = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/indicators/vnf_finished.csv"

    generate_bash_script(app, checkpointInterval, tthread, scheduler, defaultScheduler, complexity, NUM_ITEMS, rootFilePath, totalEvents, nfvWorkloadPath, communicationChoice, vnfInstanceNum, offloadCCThreadNum, offloadLockNum, rRatioSharedReaders, wRatioSharedWriters, rwRatioMutualInteractive, ccStrategy, workloadPattern, enableCCSwitch, experimentID, script_path)

    execute_bash_script(script_path, vnf_finished_indicator)

    root_directory = "/home/shuhao/DB4NFV/morphStream/scripts/TransNFV/experiments/pre_study"
    read_and_plot(root_directory)
