#!/bin/bash

source ../../global.sh || exit


function ResetParameters() {
  app="StreamLedger"
  checkpointInterval=10240
  tthread=24
  scheduler="OG_BFS_A"
  defaultScheduler="OG_BFS_A"
  CCOption=3 #TSTREAM
  complexity=8000
  NUM_ITEMS=491520
  deposit_ratio=100
  key_skewness=0


  isCyclic=0
  isDynamic=0
  workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
  schedulerPool="OG_BFS_A,OG_NS_A,OP_NS_A,OP_NS"
  rootFilePath="${project_Dir}/result/data/MicroArchitecturalAnalysis"
  shiftRate=1
  totalEvents=`expr $checkpointInterval \* $tthread \* 13 \* $shiftRate`
}

function runTStream() {
  echo "java -Xms300g -Xmx300g -jar -d64 ${jar_Dir} \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --tthread $tthread \
          --scheduler $scheduler \
          --defaultScheduler $defaultScheduler \
          --checkpoint_interval $checkpointInterval \
          --CCOption $CCOption \
          --complexity $complexity \
          --deposit_ratio $deposit_ratio \
          --isCyclic $isCyclic \
          --rootFilePath $rootFilePath \
          --isDynamic $isDynamic \
          --totalEvents $totalEvents \
          --shiftRate $shiftRate \
          --workloadType $workloadType \
          --schedulerPool $schedulerPool"
  java -Xms300g -Xmx300g -Xss100M -jar -d64 ${jar_Dir} \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --defaultScheduler $defaultScheduler \
    --checkpoint_interval $checkpointInterval \
    --CCOption $CCOption \
    --complexity $complexity \
    --deposit_ratio $deposit_ratio \
    --isCyclic $isCyclic \
    --rootFilePath $rootFilePath \
    --isDynamic $isDynamic \
    --totalEvents $totalEvents \
    --shiftRate $shiftRate \
    --workloadType $workloadType \
    --schedulerPool $schedulerPool
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  isDynamic=1
  # runTStream
  ResetParameters

  scheduler=TStream
  #workloadType="default"
  isDynamic=0
  runTStream
}

function dynamic_runner() { # multi-batch exp
 ResetParameters
 app=StreamLedger
 baselineEvaluation
}

dynamic_runner
