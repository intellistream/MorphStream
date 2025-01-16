import argparse
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn2pmml import PMMLPipeline, sklearn2pmml
from sklearn.metrics import accuracy_score
import argparse
import subprocess
import os
import threading
import matplotlib.pyplot as plt
import itertools
import pandas as pd
import numpy as np

from pypmml import Model


def read_throughput(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy, udfComplexity):
    throughput_file_path = f"{exp_dir}/results/{expID}/vnfID={vnfID}/numPackets={numPackets}/numInstances={numInstances}/" \
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


def generate_bash_script(app, expID, vnfID, expDir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy,
                         doMVCC, udfComplexity,
                         keySkew, workloadSkew, readRatio, locality, scopeRatio,
                         script_path, root_dir):
    script_content = f"""#!/bin/bash

function ResetParameters() {{
  app="{app}"
  expID="{expID}"
  vnfID="{vnfID}"
  nfvExperimentPath="{expDir}"
  numPackets={numPackets}
  numItems={numItems}
  numInstances={numInstances}
  numTPGThreads={numTPGThreads}
  numOffloadThreads={numOffloadThreads}
  puncInterval={puncInterval}
  ccStrategy="{ccStrategy}"
  doMVCC={doMVCC}
  udfComplexity={udfComplexity}
  keySkew={keySkew}
  workloadSkew={workloadSkew}
  readRatio={readRatio}
  locality={locality}
  scopeRatio={scopeRatio}
}}

function runTStream() {{
  echo "java -Xms100g -Xmx100g -Xss10M -jar {root_dir}/morphStream/morph-clients/target/morph-clients-0.1.jar \\
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
  java -Xms100g -Xmx100g -Xss10M -jar {root_dir}/morphStream/morph-clients/target/morph-clients-0.1.jar \\
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
    runTStream
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



vnfID = 11
numItems = 1000
numPackets = 400000
numInstances = 4
app = "nfv_test"
expID = '6_Testing'


# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
doMVCC = 0
udfComplexityList = [2, 5, 10]

keySkewList = [0]
workloadSkewList = [0]

# readRatioList = [0, 50, 100]
# localityList = [0, 50, 100]
# scopeRatioList = [0, 50, 100]

# readRatioList = [0]
# localityList = [0, 50, 100]
# scopeRatioList = [0]

readRatioList = [0, 50, 100]
localityList = [0, 50, 100]
scopeRatioList = [0, 50]


testGTModuleMap = {} # (workload characteristics) -> module configuration
testGThroughputMap = {} # (workload characteristics) -> throughput
testPredictionModuleMap = {}
testPredictionThroughputMap = {}

# keySkew, workloadSkew, readRatio, locality, scopeRatio, optimal_strategy
def run_test_GT_experiment(root_dir, exp_dir, udfComplexity):
    for keySkew, workloadSkew, readRatio, locality, scopeRatio in itertools.product(keySkewList, workloadSkewList, readRatioList, localityList, scopeRatioList):
        for ccStrategy in ["Offloading", "Replication", "Partitioning"]:
            shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
            generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                                 numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity,
                                 keySkew, workloadSkew, readRatio, locality, scopeRatio,
                                 shellScriptPath, root_dir)
            execute_bash_script(shellScriptPath)

    for keySkew, workloadSkew, readRatio, locality, scopeRatio in itertools.product(keySkewList, workloadSkewList, readRatioList, localityList, scopeRatioList):
        maxThroughput = 0
        optimalModule = "Default"
        for ccStrategy in ["Offloading", "Replication", "Partitioning"]:
            currentThroughput = read_throughput(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, ccStrategy, udfComplexity)
            if currentThroughput > maxThroughput:
                maxThroughput = currentThroughput
                optimalModule = ccStrategy
        testGTModuleMap[(keySkew, workloadSkew, readRatio, locality, scopeRatio, udfComplexity)] = optimalModule
        testGThroughputMap[(keySkew, workloadSkew, readRatio, locality, scopeRatio, udfComplexity)] = maxThroughput

    label_file_path = f"{exp_dir}/training_data/testing_GT_module.csv"
    throughput_file_path = f"{exp_dir}/training_data/testing_GT_throughput.csv"
    with open(label_file_path, 'w') as f:
        for key, value in testGTModuleMap.items():
            f.write(
                f"{key[0]},{key[1]},{key[2]},{key[3]},{key[4]},{key[5]},{value}\n")  # keySkew, workloadSkew, readRatio, locality, scopeRatio, udfComplexity, optimal_strategy
    print(f"Ground truth labels written to {label_file_path}")

    with open(throughput_file_path, 'w') as f:
        for key, value in testGThroughputMap.items():
            f.write(
                f"{key[0]},{key[1]},{key[2]},{key[3]},{key[4]},{key[5]},{value}\n")  # keySkew, workloadSkew, readRatio, locality, scopeRatio, udfComplexity, optimal_strategy
    print(f"Ground truth throughput written to {throughput_file_path}")



def run_test_prediction_experiment(root_dir, exp_dir, udfComplexity, model):
    for keySkew, workloadSkew, readRatio, locality, scopeRatio in itertools.product(keySkewList, workloadSkewList, readRatioList, localityList, scopeRatioList):
        result = model.predict({'keySkew': keySkew, 'workloadSkew': workloadSkew, 'readRatio': readRatio,
                                'locality': locality, 'scopeRatio': scopeRatio})

        predicted_optimal_strategy = max(result, key=result.get)
        predicted_optimal_strategy = predicted_optimal_strategy.replace('probability(', '').replace(')', '')

        shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
        generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                             numTPGThreads, numOffloadThreads, puncInterval, predicted_optimal_strategy, doMVCC, udfComplexity,
                             keySkew, workloadSkew, readRatio, locality, scopeRatio,
                             shellScriptPath, root_dir)
        execute_bash_script(shellScriptPath)

        throughput = read_throughput(exp_dir, expID, keySkew, workloadSkew, readRatio, locality, scopeRatio, predicted_optimal_strategy, udfComplexity)
        testPredictionModuleMap[(keySkew, workloadSkew, readRatio, locality, scopeRatio, udfComplexity)] = predicted_optimal_strategy
        testPredictionThroughputMap[(keySkew, workloadSkew, readRatio, locality, scopeRatio, udfComplexity)] = throughput

    pred_label_file_path = f"{exp_dir}/training_data/testing_pred_module.csv"
    pred_throughput_file_path = f"{exp_dir}/training_data/testing_pred_throughput.csv"
    with open(pred_label_file_path, 'w') as f:
        for key, value in testPredictionModuleMap.items():
            f.write(
                f"{key[0]},{key[1]},{key[2]},{key[3]},{key[4]},{key[5]},{value}\n")  # keySkew, workloadSkew, readRatio, locality, scopeRatio, udfComplexity, optimal_strategy
    print(f"Pred labels written to {pred_label_file_path}")

    with open(pred_throughput_file_path, 'w') as f:
        for key, value in testPredictionThroughputMap.items():
            f.write(
                f"{key[0]},{key[1]},{key[2]},{key[3]},{key[4]},{key[5]},{value}\n")  # keySkew, workloadSkew, readRatio, locality, scopeRatio, udfComplexity, optimal_strategy
    print(f"Pred throughput written to {pred_throughput_file_path}")


def draw_throughput_line_chart(exp_dir):
    groundtruth_file = f"{exp_dir}/training_data/testing_GT_throughput.csv"
    predicted_file = f"{exp_dir}/training_data/testing_pred_throughput.csv"
    groundtruth_df = pd.read_csv(groundtruth_file, header=None)
    predicted_df = pd.read_csv(predicted_file, header=None)

    columns = ['w1', 'w2', 'w3', 'w4', 'w5', 'complexity', 'throughput']
    groundtruth_df.columns = columns
    predicted_df.columns = columns

    groundtruth_df['workload_index'] = groundtruth_df[['w1', 'w2', 'w3', 'w4', 'w5']].apply(tuple, axis=1).rank(
        method='dense').astype(int)
    predicted_df['workload_index'] = predicted_df[['w1', 'w2', 'w3', 'w4', 'w5']].apply(tuple, axis=1).rank(
        method='dense').astype(int)
    assert groundtruth_df['workload_index'].equals(predicted_df['workload_index']), "Workload indexes do not match!"

    complexities = sorted(groundtruth_df['complexity'].unique())
    vnf_names = ["NAT", "IPS", "TD"]

    plt.figure(figsize=(12, 4.5))
    colors = plt.cm.tab10(np.linspace(0, 1, len(complexities)))

    # Plot ground truth lines
    for i, complexity in enumerate(complexities):
        gt_data = groundtruth_df[groundtruth_df['complexity'] == complexity]
        vnf_name = vnf_names[i]
        plt.plot(gt_data['workload_index'], gt_data['throughput'], '-o', color=colors[i],
                 label=f"{vnf_name} (GT)")

    # Plot predicted lines
    for i, complexity in enumerate(complexities):
        pred_data = predicted_df[predicted_df['complexity'] == complexity]
        vnf_name = vnf_names[i]
        plt.plot(pred_data['workload_index'], pred_data['throughput'], '--x', color=colors[i],
                 label=f"{vnf_name} (Pred)")

    plt.xlabel("Workload Phase Number", fontsize=18)
    plt.ylabel("Throughput (M req/sec)", fontsize=18)
    # Set x-axis ticks to only show indices with actual data
    unique_indices = groundtruth_df['workload_index'].unique()
    plt.xticks(ticks=unique_indices, labels=unique_indices, fontsize=14)
    plt.yticks(fontsize=14)

    plt.legend(bbox_to_anchor=(0.5, 1.22), loc='upper center', ncol=6, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    plt.tight_layout()
    plt.subplots_adjust(left=0.1, right=0.98, top=0.85, bottom=0.15)

    figure_dir = os.path.join(exp_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, '6_model_evaluation.pdf'))
    print(f"Figure saved to {figure_dir}/6_model_evaluation.pdf")



def main(root_dir, exp_dir):
    print(f"Root directory: {root_dir}")
    print(f"Experiment directory: {exp_dir}")
    model_dir = f'{exp_dir}/training_data/mlp_model.pmml'
    model = Model.load(model_dir)

    for udfComplexity in udfComplexityList:
        run_test_GT_experiment(root_dir, exp_dir, udfComplexity)
        run_test_prediction_experiment(root_dir, exp_dir, udfComplexity, model)

    draw_throughput_line_chart(exp_dir)

    # print(testGTModuleMap)
    # print("\n")
    # print(testGThroughputMap)
    # print("\n")
    # print(testPredictionModuleMap)
    # print("\n")
    # print(testPredictionThroughputMap)

    print("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)
    # print("MLP model trained and exported.")