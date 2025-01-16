import argparse
import subprocess
import os
import threading
import pandas as pd
import matplotlib.pyplot as plt
import itertools

from pypmml import Model




def generate_bash_script(app, expID, vnfID, expDir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy,
                         doMVCC, udfComplexity,
                         keySkewList, workloadSkewList, readRatioList, localityList, scopeRatioList,
                         script_path, root_dir):
    
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
  keySkew=0  # Default value, will be updated in loop
  workloadSkew=0
  readRatio=0  # Default value, will be updated in loop
  locality=0  # Default value, will be updated in loop
  scopeRatio=0
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
                        for ccStrategy in Partitioning Replication Offloading
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

    

vnfID = 11
numItems = 1000
numPackets = 400000
numInstances = 4
app = "nfv_test"
expID = '6_Training'


# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
doMVCC = 0
udfComplexity = 10
ccStrategy = "Offloading"

keySkewList = [0]
workloadSkewList = [0]
# readRatioList = [0, 50, 100]
# scopeRatioList = [0, 50, 100]
# localityList = [0, 50, 100]
# readRatioList = [0]
# scopeRatioList = [0]
# localityList = [0, 50, 100]
readRatioList = [0, 25, 50, 75, 100]
localityList = [0, 25, 50, 75, 100]
scopeRatioList = [0, 25, 50, 75, 100]


def run_dynamic_workload(root_dir, exp_dir):
    shellScriptPath = os.path.join(exp_dir, "shell_scripts", f"{expID}.sh")
    generate_bash_script(app, expID, vnfID, exp_dir, numPackets, numItems, numInstances,
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity,
                         keySkewList, workloadSkewList, readRatioList, localityList, scopeRatioList,
                         shellScriptPath, root_dir)
    execute_bash_script(shellScriptPath)


def main(root_dir, exp_dir):
    print(f"Root directory: {root_dir}")
    print(f"Experiment directory: {exp_dir}")
    run_dynamic_workload(root_dir, exp_dir)

    print("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)
    print("Preliminary study results generated")