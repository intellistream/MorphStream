import subprocess
import os
import time
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import csv


def generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, 
                         doMVCC, udfComplexity, 
                         keySkewList, workloadSkew, readRatioList, localityList, scopeRatio, 
                         script_path):
    
    # Convert Python lists to space-separated strings for shell script
    keySkewList_str = " ".join(map(str, keySkewList))
    readRatioList_str = " ".join(map(str, readRatioList))
    localityList_str = " ".join(map(str, localityList))
    
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
  workloadSkew={workloadSkew}
  readRatio=0  # Default value, will be updated in loop
  locality=0  # Default value, will be updated in loop
  scopeRatio={scopeRatio}
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
  readRatioList=({readRatioList_str})
  localityList=({localityList_str})

  for keySkew in "${{keySkewList[@]}}"
  do
    for readRatio in "${{readRatioList[@]}}"
    do
      for locality in "${{localityList[@]}}"
      do
        for ccStrategy in Partitioning Replication Offloading Proactive OpenNF CHC S6
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
}}

Per_Phase_Experiment

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



vnfID = 11
numItems = 10000
numPackets = 400000
numInstances = 4

# System params
numTPGThreads = 4
numOffloadThreads = 4
puncInterval = 1000
ccStrategy = "Offloading"
doMVCC = 0
udfComplexity = 10


def phase1():
    # Basic params
    app = "nfv_test"
    expID = "5.2.2_phase1"

    # Workload chars
    Phase1_keySkewList = [0]
    Phase1_workloadSkew = 0
    Phase1_readRatioList = [50]
    Phase1_localityList = [75, 80, 90, 100]
    Phase1_scopeRatio = 0

    rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID

    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         Phase1_keySkewList, Phase1_workloadSkew, Phase1_readRatioList, Phase1_localityList, Phase1_scopeRatio, 
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)

def phase2():
    # Basic params
    app = "nfv_test"
    expID = "5.2.2_phase2"

    # Workload chars
    Phase2_keySkewList = [0, 50, 100, 150]
    Phase2_workloadSkew = 0
    Phase2_readRatioList = [75, 80, 90, 100]
    Phase2_localityList = [0]
    Phase2_scopeRatio = 0

    rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID

    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         Phase2_keySkewList, Phase2_workloadSkew, Phase2_readRatioList, Phase2_localityList, Phase2_scopeRatio, 
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)

def phase3():
    # Basic params
    app = "nfv_test"
    expID = '5.2.2_phase3'

    # Workload chars
    Phase3_keySkewList = [0, 50, 100, 150]
    Phase3_workloadSkew = 0
    Phase3_readRatioList = [0, 10, 20, 25]
    Phase3_localityList = [0]
    Phase3_scopeRatio = 0

    rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID

    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         Phase3_keySkewList, Phase3_workloadSkew, Phase3_readRatioList, Phase3_localityList, Phase3_scopeRatio, 
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)


def phase4():
    # Basic params
    app = "nfv_test"
    expID = "5.2.2_phase4"

    # Workload chars
    Phase4_keySkewList = [0, 50, 100, 150]
    Phase4_workloadSkew = 0
    Phase4_readRatioList = [0, 25, 50, 75, 100]
    Phase4_localityList = [0]
    Phase4_scopeRatio = 0

    rootDir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    shellScriptPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/shell_scripts/%s.sh" % expID

    generate_bash_script(app, expID, vnfID, rootDir, numPackets, numItems, numInstances, 
                         numTPGThreads, numOffloadThreads, puncInterval, ccStrategy, doMVCC, udfComplexity, 
                         Phase4_keySkewList, Phase4_workloadSkew, Phase4_readRatioList, Phase4_localityList, Phase4_scopeRatio, 
                         shellScriptPath)
    
    execute_bash_script(shellScriptPath)



if __name__ == "__main__":
    # Basic params
    phase1()
    # phase2()
    # phase3()
    # phase4()