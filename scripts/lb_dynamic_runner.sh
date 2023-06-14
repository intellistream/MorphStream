#!/bin/bash

function ResetParameters() {
  local inputScheduler=$1
  local inputNewConnRatio=$2

  app="LoadBalancer"
  checkpointInterval=10240
  tthread=24
  scheduler=$inputScheduler
  defaultScheduler=$inputScheduler
  CCOption=3 #TSTREAM
  complexity=10000
  NUM_ITEMS=245760
  deposit_ratio=95
  key_skewness=0


  isCyclic=0
  isDynamic=0
  workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
  schedulerPool="OG_DFS_A,OG_NS_A,OP_NS_A,OP_NS"
  newConnRatio=$inputNewConnRatio
  rootFilePath="/Users/zhonghao/data"
  shiftRate=1
  totalEvents=`expr $checkpointInterval \* $tthread \* 13 \* $shiftRate`
}

function runTStream() {
  echo "java -Xms300g -Xmx300g -jar -d64 /Users/zhonghao/Documents/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
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
  java -Xms300g -Xmx300g -Xss100M -XX:+PrintGCDetails -Xmn150g -XX:+UseG1GC -jar -d64 /Users/zhonghao/Documents/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
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
function patEvluation() {
  isDynamic=0
  CCOption=4 #SSTORE
  runTStream
}

function dynamic_runner() { # multi-batch exp
  # Array of different scheduler and ratio of new connections
  schedulerArray=("OP_BFS_A" "OG_BFS_A" "OP_NS_A" "OG_NS_A")
  newConnRatioArray=(10 20 30 40 50 60 70 80)

  # LB Experiment on MorphStream with different scheduling decisions & new_conn_ratio
  for scheduler in "${schedulerArray[@]}"; do
    for newConnRatio in "${newConnRatioArray[@]}"; do
      ResetParameters "$scheduler" "$newConnRatio"
      baselineEvaluation
    done
  done

  # LB Experiment on SStore with different new_conn_ratio
  for newConnRatio in "${newConnRatioArray[@]}"; do
    ResetParameters "" "$newConnRatio" # only change new_conn_ratio
    patEvluation
  done
}


dynamic_runner
#ResetParameters