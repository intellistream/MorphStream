#!/bin/bash

function ResetParameters() {
  local inputScheduler=$1
  local inputNewConnRatio=$2
  local numItems=$3
  local checkpointInterval=$4

  app="LoadBalancer"
  checkpointInterval=$checkpointInterval
  tthread=24
  scheduler=$inputScheduler
  defaultScheduler=$inputScheduler
  CCOption=3 #TSTREAM
  complexity=0
  NUM_ITEMS=$numItems
  deposit_ratio=95
  key_skewness=0


  isCyclic=0
  isDynamic=0
  workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
  schedulerPool="OG_DFS_A,OG_NS_A,OP_NS_A,OP_NS"
  newConnRatio=$inputNewConnRatio
  rootFilePath="/Users/zhonghao/data"
  shiftRate=1
  totalEvents=$numItems
}

function runTStream() {
  echo "java -Xms300g -Xmx300g -jar -d64 /application/target/application-0.0.2-jar-with-dependencies.jar \
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
  java -Xms300g -Xmx300g -Xss100M -XX:+PrintGCDetails -Xmn150g -XX:+UseG1GC -jar -d64 /application/target/application-0.0.2-jar-with-dependencies.jar \
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

# run basic experiment for SStore
function patEvaluation() {
  isDynamic=0
  CCOption=4 #SSTORE
  runTStream
}

# run basic experiment for SStore
function noccEvaluation() {
  isDynamic=0
  CCOption=0 #NOCC
  runTStream
}

function dynamic_runner() { # multi-batch exp
  # Array of different scheduler and ratio of new connections
  schedulerArray=("OP_BFS_A" "OG_BFS_A" "OP_NS_A" "OG_NS_A")
  newConnRatioArray=(5 10 20 40 60)
  numItemsArray=(7200 14400 72000)
  tthread=24

  # LB Experiment on MorphStream with different scheduling decisions & new_conn_ratio
  for scheduler in "${schedulerArray[@]}"; do
    for newConnRatio in "${newConnRatioArray[@]}"; do
      for numItems in "${numItemsArray[@]}"; do
        ResetParameters "$scheduler" "$newConnRatio" "$numItems" "300"
        baselineEvaluation
      done
    done
  done

  # LB Experiment on SStore with different new_conn_ratio
  for newConnRatio in "${newConnRatioArray[@]}"; do
    for numItems in "${numItemsArray[@]}"; do
      ResetParameters "" "$newConnRatio" "$numItems" "$(expr $numItems / $tthread)"
      patEvaluation
    done
  done

  # LB Experiment on NOCC with different new_conn_ratio
  for newConnRatio in "${newConnRatioArray[@]}"; do
    for numItems in "${numItemsArray[@]}"; do
      ResetParameters "" "$newConnRatio" "$numItems" "$(expr $numItems / $tthread)"
      noccEvaluation
    done
  done
}


dynamic_runner
#ResetParameters