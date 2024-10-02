import subprocess
import os
import time
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import csv
import itertools

from pypmml import Model




def generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, 
                         doMVCC, udfComplexity, 
                         keySkewList, workloadSkewList, readRatioList, localityList, scopeRatioList, 
                         script_path):
    
    keySkewList_str = " ".join(map(str, keySkewList))
    workloadSkewList_str = " ".join(map(str, workloadSkewList))
    readRatioList_str = " ".join(map(str, readRatioList))
    localityList_str = " ".join(map(str, localityList))
    scopeRatioList_str = " ".join(map(str, scopeRatioList))
    
    script_content = f"""#!/bin/bash

function ResetParameters() {{
  app="{app}"
  expID="{expID}"
  vnfID="{vnfID}"
  nfvExperimentPath="{rootDir}"
  numPackets={numPackets}
  numItems={numItems}
  numInstances={numInstances}
  numTPGThreads={numTPGThreads}
  numOffloadThreads={numOffloadThreads}
  puncInterval={puncInterval}
  ccStrategy="{ccStrategy}"
  doMVCC={doMVCC}
  udfComplexity={udfComplexity}
  keySkew=0  # Default value, will be updated in loop
  workloadSkew=0
  readRatio=0  # Default value, will be updated in loop
  locality=0  # Default value, will be updated in loop
  scopeRatio=0
}}

function runTStream() {{
  echo "java -Xms100g -Xmx100g -Xss10M -jar /home/zhonghao/IdeaProjects/transNFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
          --app $app \\
          --expID $expID \\
          --vnfID $vnfID \\
          --nfvExperimentPath $nfvExperimentPath \\
          --numPackets $numPackets \\
          --numItems $numItems \\
          --numInstances $numInstances \\
          --numTPGThreads $numTPGThreads \\
          --numOffloadThreads $numOffloadThreads \\
          --puncInterval $puncInterval \\
          --ccStrategy $ccStrategy \\
          --doMVCC $doMVCC \\
          --udfComplexity $udfComplexity \\
          --keySkew $keySkew \\
          --workloadSkew $workloadSkew \\
          --readRatio $readRatio \\
          --locality $locality \\
          --scopeRatio $scopeRatio
          "
  java -Xms100g -Xmx100g -Xss10M -jar /home/zhonghao/IdeaProjects/transNFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
    --app $app \\
    --expID $expID \\
    --vnfID $vnfID \\
    --nfvExperimentPath $nfvExperimentPath \\
    --numPackets $numPackets \\
    --numItems $numItems \\
    --numInstances $numInstances \\
    --numTPGThreads $numTPGThreads \\
    --numOffloadThreads $numOffloadThreads \\
    --puncInterval $puncInterval \\
    --ccStrategy $ccStrategy \\
    --doMVCC $doMVCC \\
    --udfComplexity $udfComplexity \\
    --keySkew $keySkew \\
    --workloadSkew $workloadSkew \\
    --readRatio $readRatio \\
    --locality $locality \\
    --scopeRatio $scopeRatio
}}

function Per_Phase_Experiment() {{
    ResetParameters
    keySkewList=({keySkewList_str})
    workloadSkewList=({workloadSkewList_str})
    readRatioList=({readRatioList_str})
    localityList=({localityList_str})
    scopeRatioList=({scopeRatioList_str})

    for keySkew in "${{keySkewList[@]}}"
    do
        for workloadSkew in "${{workloadSkewList[@]}}"
        do
            for readRatio in "${{readRatioList[@]}}"
            do
                for locality in "${{localityList[@]}}"
                do
                    for scopeRatio in "${{scopeRatioList[@]}}"
                    do
                        for ccStrategy in Partitioning Replication Offloading Proactive
                        do
                            keySkew=$keySkew
                            readRatio=$readRatio
                            locality=$locality
                            ccStrategy=$ccStrategy

                            runTStream
                        done
                    done
                done
            done
        done
    done
}}

Per_Phase_Experiment

"""

    with open(script_path, "w") as file:
        file.write(script_content)
    os.chmod(script_path, 0o755)

def stream_reader(pipe, pipe_name):
    with pipe:
        for line in iter(pipe.readline, ''):
            print(f"{pipe_name}: {line.strip()}")

def execute_bash_script(script_path):
    print(f"Executing bash script: {script_path}")
    process = subprocess.Popen(["bash", script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    stdout_thread = threading.Thread(target=stream_reader, args=(process.stdout, "STDOUT"))
    stderr_thread = threading.Thread(target=stream_reader, args=(process.stderr, "STDERR"))
    stdout_thread.start()
    stderr_thread.start()

    process.wait()
    stdout_thread.join()
    stderr_thread.join()

    if process.returncode != 0:
        print(f"Bash script finished with errors.")
    else:
        print(f"Bash script completed successfully.")


def generate_bash_script_inference(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, 
                         doMVCC, udfComplexity, 
                         keySkewList, workloadSkewList, readRatioList, localityList, scopeRatioList, 
                         script_path):
    
    keySkewList_str = " ".join(map(str, keySkewList))
    workloadSkewList_str = " ".join(map(str, workloadSkewList))
    readRatioList_str = " ".join(map(str, readRatioList))
    localityList_str = " ".join(map(str, localityList))
    scopeRatioList_str = " ".join(map(str, scopeRatioList))
    
    script_content = f"""#!/bin/bash

function ResetParameters() {{
  app="{app}"
  expID="{expID}"
  vnfID="{vnfID}"
  nfvExperimentPath="{rootDir}"
  numPackets={numPackets}
  numItems={numItems}
  numInstances={numInstances}
  numTPGThreads={numTPGThreads}
  numOffloadThreads={numOffloadThreads}
  puncInterval={puncInterval}
  ccStrategy="{ccStrategy}"
  doMVCC={doMVCC}
  udfComplexity={udfComplexity}
  keySkew=0  # Default value, will be updated in loop
  workloadSkew=0
  readRatio=0  # Default value, will be updated in loop
  locality=0  # Default value, will be updated in loop
  scopeRatio=0
}}

function runTStream() {{
  echo "java -Xms100g -Xmx100g -Xss10M -jar /home/zhonghao/IdeaProjects/transNFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
          --app $app \\
          --expID $expID \\
          --vnfID $vnfID \\
          --nfvExperimentPath $nfvExperimentPath \\
          --numPackets $numPackets \\
          --numItems $numItems \\
          --numInstances $numInstances \\
          --numTPGThreads $numTPGThreads \\
          --numOffloadThreads $numOffloadThreads \\
          --puncInterval $puncInterval \\
          --ccStrategy $ccStrategy \\
          --doMVCC $doMVCC \\
          --udfComplexity $udfComplexity \\
          --keySkew $keySkew \\
          --workloadSkew $workloadSkew \\
          --readRatio $readRatio \\
          --locality $locality \\
          --scopeRatio $scopeRatio
          "
  java -Xms100g -Xmx100g -Xss10M -jar /home/zhonghao/IdeaProjects/transNFV/morphStream/morph-clients/target/morph-clients-0.1.jar \\
    --app $app \\
    --expID $expID \\
    --vnfID $vnfID \\
    --nfvExperimentPath $nfvExperimentPath \\
    --numPackets $numPackets \\
    --numItems $numItems \\
    --numInstances $numInstances \\
    --numTPGThreads $numTPGThreads \\
    --numOffloadThreads $numOffloadThreads \\
    --puncInterval $puncInterval \\
    --ccStrategy $ccStrategy \\
    --doMVCC $doMVCC \\
    --udfComplexity $udfComplexity \\
    --keySkew $keySkew \\
    --workloadSkew $workloadSkew \\
    --readRatio $readRatio \\
    --locality $locality \\
    --scopeRatio $scopeRatio
}}

function Per_Phase_Experiment() {{
    ResetParameters
    keySkewList=({keySkewList_str})
    workloadSkewList=({workloadSkewList_str})
    readRatioList=({readRatioList_str})
    localityList=({localityList_str})
    scopeRatioList=({scopeRatioList_str})

    for keySkew in "${{keySkewList[@]}}"
    do
        for workloadSkew in "${{workloadSkewList[@]}}"
        do
            for readRatio in "${{readRatioList[@]}}"
            do
                for locality in "${{localityList[@]}}"
                do
                    for scopeRatio in "${{scopeRatioList[@]}}"
                    do
                        keySkew=$keySkew
                        readRatio=$readRatio
                        locality=$locality
                        ccStrategy=$ccStrategy

                        runTStream
                    done
                done
            done
        done
    done
}}

Per_Phase_Experiment

"""

    with open(script_path, "w") as file:
        file.write(script_content)
    os.chmod(script_path, 0o755)

def stream_reader_inference(pipe, pipe_name):
    with pipe:
        for line in iter(pipe.readline, ''):
            print(f"{pipe_name}: {line.strip()}")

def execute_bash_script_inference(script_path):
    print(f"Executing bash script: {script_path}")
    process = subprocess.Popen(["bash", script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    stdout_thread = threading.Thread(target=stream_reader_inference, args=(process.stdout, "STDOUT"))
    stderr_thread = threading.Thread(target=stream_reader_inference, args=(process.stderr, "STDERR"))
    stdout_thread.start()
    stderr_thread.start()

    process.wait()
    stdout_thread.join()
    stderr_thread.join()

    if process.returncode != 0:
        print(f"Bash script finished with errors.")
    else:
        print(f"Bash script completed successfully.")


def read_throughput(expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy):
    throughput_file_path = f"{rootDir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
                  f"numItems={numItems}/keySkew={keySkew}/workloadSkew={workloadSkew}/readRatio={readRatio}/" \
                  f"locality={locality}/scopeRatio={scopeRatio}/numTPGThreads={numTPGThreads}/" \
                  f"numOffloadThreads={numOffloadThreads}/puncInterval={puncInterval}/ccStrategy={ccStrategy}/" \
                  f"doMVCC={doMVCC}/udfComplexity={udfComplexity}/throughput.csv"
    try:
        df = pd.read_csv(throughput_file_path, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
        return df['Throughput'].iloc[0]
    except Exception as e:
        print(f"Failed to read {throughput_file_path}: {e}")
        return None

    

vnfID = 11
numItems = 10000
numPackets = 100000
numInstances = 4
app = "nfv_test"
expID = "5.5_Evaluation"
model = Model.load('/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/training_data/mlp_model.pmml')

# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
ccStrategy = "Offloading"
doMVCC = 0
udfComplexity = 10
ccStrategyList = ["Partitioning", "Replication", "Offloading", "Proactive"]
rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"

# Workload params
Phase1 = [[0], [0], [50], [75, 80, 90, 100], [0]]
Phase2 = [[0, 50], [0], [75, 100], [0], [0]]
Phase3 = [[0, 50], [0], [0, 25], [0], [0]]
Phase4 = [[0, 50], [0], [25, 75], [0], [0]]

actual_optimal_strategy_list = []
actual_optimal_throughput_list = []
predicted_optimal_strategy_list = []
predicted_throughput_list = []

def generate_workload_tuples():
    phase_params = [Phase1, Phase2, Phase3, Phase4]
    result_list = []
    for phase in phase_params:
        combinations = list(itertools.product(*phase))
        result_list.extend(combinations)
    return result_list

all_tuples = generate_workload_tuples()





def get_actual_optimal_strategy():
    for input_data in all_tuples:
        optimal_strategy = None
        optimal_throughput = 0
        for ccStrategy in ccStrategyList:
            throughput = read_throughput(expID, input_data[0], input_data[1], input_data[2], input_data[3], input_data[4], ccStrategy)
            if throughput is not None and throughput > optimal_throughput:
                optimal_throughput = throughput
                optimal_strategy = ccStrategy
        actual_optimal_strategy_list.append(optimal_strategy)
        actual_optimal_throughput_list.append(optimal_throughput / 1000000)
    print("Actual optimal strategy:" + str(actual_optimal_strategy_list))


def phase1():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID
    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         [0], [0], [50], [75, 80, 90, 100], [0], 
                         shellScriptPath)
    execute_bash_script(shellScriptPath)

def phase2():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID
    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         [0, 50], [0], [75, 100], [0], [0], 
                         shellScriptPath)
    execute_bash_script(shellScriptPath)

def phase3():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID
    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         [0, 50], [0], [0, 25], [0], [0], 
                         shellScriptPath)
    execute_bash_script(shellScriptPath)

def phase4():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID
    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         [0, 50], [0], [25, 75], [0], [0], 
                         shellScriptPath)
    execute_bash_script(shellScriptPath)



def exp_under_inference():
    inference_exp_id = "5.5_Inference"
    inferenceShellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % inference_exp_id
    for input_data in all_tuples:
        result = model.predict({'keySkew': input_data[0], 'workloadSkew': input_data[1], 'readRatio': input_data[2], 'locality': input_data[3], 'scopeRatio': input_data[4]})
        
        predicted_optimal_strategy = max(result, key=result.get)
        predicted_optimal_strategy = predicted_optimal_strategy.replace('probability(', '').replace(')', '')

        generate_bash_script_inference(app, inference_exp_id, vnfID, rootDir, numPackets, numItems, numInstances,
                                numTPGThreads, numOffloadThreads, puncInterval, predicted_optimal_strategy, doMVCC, udfComplexity,
                                {input_data[0]}, {input_data[1]}, {input_data[2]}, {input_data[3]}, {input_data[4]},
                                inferenceShellScriptPath)
        execute_bash_script_inference(inferenceShellScriptPath)


def get_predicted_optimal_strategy():
    inference_exp_id = "5.5_Inference"
    for input_data in all_tuples:
        result = model.predict({'keySkew': input_data[0], 'workloadSkew': input_data[1], 'readRatio': input_data[2], 'locality': input_data[3], 'scopeRatio': input_data[4]})
        
        predicted_optimal_strategy = max(result, key=result.get)
        predicted_optimal_strategy = predicted_optimal_strategy.replace('probability(', '').replace(')', '')
        throughput = read_throughput(inference_exp_id, input_data[0], input_data[1], input_data[2], input_data[3], input_data[4], predicted_optimal_strategy)

        predicted_optimal_strategy_list.append(predicted_optimal_strategy)
        predicted_throughput_list.append(throughput / 1000000)
    print("Predicted optimal strategy:" + str(predicted_optimal_strategy_list))


def plot_throughput_comparison():
    colors = ['#8c0b0b', '#0060bf']
    fig, ax = plt.subplots(figsize=(8, 4))

    # Modify the x-axis to start from 1 instead of 0
    x_values = range(1, len(actual_optimal_throughput_list) + 1)

    plt.plot(x_values, actual_optimal_throughput_list, label="Actual Optimal", 
             color="blue", marker='o', linestyle='-', markersize=6)

    # Plot the predicted optimal strategy throughput (red line with square markers)
    plt.plot(x_values, predicted_throughput_list, label="Predicted Optimal", 
             color="red", marker='s', linestyle='--', markersize=6)

    plt.xticks(fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel("Dynamic Workload Phases", fontsize=18)
    plt.ylabel("Throughput (M Req/sec)", fontsize=18)

    # Show the plot
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    handles = [plt.Line2D([0], [0], color=color, lw=10) for color in colors]
    plt.legend(bbox_to_anchor=(0.5, 1.25), loc='upper center', ncol=2, fontsize=16, columnspacing=0.5)
    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.95, top=0.85, bottom=0.2)

    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.5_modelEval_range{numItems}_complexity{udfComplexity}.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))


if __name__ == "__main__":
    # phase1()
    # phase2()
    # phase3()
    # phase4()
    # exp_under_inference()
    get_predicted_optimal_strategy()
    get_actual_optimal_strategy()
    plot_throughput_comparison()
    print("Done")