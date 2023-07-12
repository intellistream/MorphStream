#!/bin/bash

function ResetParameters() {
  local inputScheduler=$1
  local inputNewConnRatio=$2
  local numItems=$3
  local interval=$4

  app="EventDetection"
  checkpointInterval=$interval
  tthread=4
  scheduler=$inputScheduler
  defaultScheduler=$inputScheduler
  CCOption=3 #MorphStream
  complexity=0
  NUM_ITEMS=$numItems
  deposit_ratio=95
  key_skewness=0

  isCyclic=0
  isDynamic=0
  workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
  schedulerPool="OG_DFS_A,OG_NS_A,OP_NS_A,OP_NS"
  newConnRatio=$inputNewConnRatio
  rootFilePath="/home/shuhao/data"
  shiftRate=1
  totalEvents=$numItems
}

function runTStream() {
  echo "java -Xms300g -Xmx300g -jar -d64 /home/shuhao/IdeaProjects/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --tthread $tthread \
          --scheduler $scheduler \
          --defaultScheduler $defaultScheduler \
          --checkpoint_interval $checkpointInterval \
          --CCOption $CCOption \
          --complexity $complexity \
          --deposit_ratio $deposit_ratio \
          --key_skewness $key_skewness \
          --isCyclic $isCyclic \
          --rootFilePath $rootFilePath \
          --isDynamic $isDynamic \
          --totalEvents $totalEvents \
          --shiftRate $shiftRate \
          --workloadType $workloadType \
          --schedulerPool $schedulerPool \
          --newConnRatio $newConnRatio"
  java -Xms300g -Xmx300g -Xss100M -XX:+PrintGCDetails -Xmn150g -XX:+UseG1GC -jar -d64 /home/shuhao/IdeaProjects/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --defaultScheduler $defaultScheduler \
    --checkpoint_interval $checkpointInterval \
    --CCOption $CCOption \
    --complexity $complexity \
    --deposit_ratio $deposit_ratio \
    --key_skewness $key_skewness \
    --isCyclic $isCyclic \
    --rootFilePath $rootFilePath \
    --isDynamic $isDynamic \
    --totalEvents $totalEvents \
    --shiftRate $shiftRate \
    --workloadType $workloadType \
    --schedulerPool $schedulerPool \
    --newConnRatio $newConnRatio
}

# run basic experiment for MorphStream
function baselineEvaluation() {
  isDynamic=0
  CCOption=3 #MorphStream
  runTStream
}

function dynamic_runner() { # multi-batch exp
  # ED Experiment on MorphStream with various scheduling strategies
  schedulerArray=("OP_BFS_A" "OG_BFS_A" "OP_NS_A" "OG_NS_A")
  for scheduler in "${schedulerArray[@]}"; do
    ResetParameters "$scheduler" "10" "32000" "100"
    baselineEvaluation
  done
}


dynamic_runner
#ResetParameters