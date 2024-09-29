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



# Common Parameters
puncInterval = 1000 # Used to normalize workload distribution among instances 
vnfID = 11
numPackets = 100000
numInstances = 4
numItems = 10000
app = "nfv_test"

numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
ccStrategy = "Offloading"
doMVCC = 0
udfComplexity = 10
ccStrategyList = ["Partitioning", "Replication", "Offloading", "Proactive"]
rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"

# Per-phase Parameters

# Phase 1: Mostly per-flow, balanced / high skewness, read-write balanced
Phase1_expID = '5.5'
Phase1_keySkewList = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
Phase1_workloadSkewList = [0, 25, 50, 75, 100]
Phase1_readRatioList = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
Phase1_localityList = [0]
Phase1_scopeRatioList = [0]

# Phase1_expID = '5.5'
# Phase1_keySkewList = [0]
# Phase1_workloadSkewList = [0]
# Phase1_readRatioList = [0]
# Phase1_localityList = [0]
# Phase1_scopeRatioList = [0]

# Phase 2: Mostly cross-partition, balanced / high skewness, mostly read-only
Phase2_expID = '5.5'
Phase2_keySkewList = [0]
Phase2_workloadSkew = [0]
Phase2_readRatioList = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
Phase2_localityList = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
Phase2_scopeRatio = [0]



def keySkewExp():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % Phase1_expID
        
    generate_bash_script(app, Phase1_expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         Phase1_keySkewList, Phase1_workloadSkewList, Phase1_readRatioList, Phase1_localityList, Phase1_scopeRatioList, 
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)


def localityExp():
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % Phase2_expID

    generate_bash_script(app, Phase2_expID, vnfID, rootDir, numPackets, numItems, numInstances,
                        numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity,
                        Phase2_keySkewList, Phase2_workloadSkew, Phase2_readRatioList, Phase2_localityList, Phase2_scopeRatio,
                        shellScriptPath)
    
    execute_bash_script(shellScriptPath)



if __name__ == "__main__":
    keySkewExp()
    # localityExp()
    print("Done")