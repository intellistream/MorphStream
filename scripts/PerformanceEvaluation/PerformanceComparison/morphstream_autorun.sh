#!/bin/bash

source ../../global.sh || exit

function ResetParameters() {
  app="StreamLedger"
  NUM_ITEMS=122880
  NUM_ACCESS=10
  checkpointInterval=10240
  tthread=24
  scheduler="OP_NS"
  deposit_ratio=25
  key_skewness=0
  overlap_ratio=0
  abort_ratio=0=100
  CCOption=3 #TSTREAM
  complexity=10000
  isCyclic=0
  rootFilePath="${project_Dir}/result/data/PerformanceComparison"
}

function runTStream() {
  totalEvents=`expr $checkpointInterval \* $tthread`
  # NUM_ITEMS=`expr $totalEvents`
  echo "java -Xms100g -Xmx100g -jar -d64 ${jar_Dir} \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --NUM_ACCESS $NUM_ACCESS \
          --tthread $tthread \
          --scheduler $scheduler \
          --totalEvents $totalEvents \
          --checkpoint_interval $checkpointInterval \
          --multiple_ratio $Ratio_of_Multiple_State_Access \
          --key_skewness $key_skewness \
          --deposit_ratio $deposit_ratio \
          --abort_ratio $abort_ratio \
          --CCOption $CCOption \
          --complexity $complexity \
          --isCyclic $isCyclic \
          --rootFilePath $rootFilePath"
  java -Xms100g -Xmx100g -Xss100M -jar -d64 $jar_Dir \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --NUM_ACCESS $NUM_ACCESS \
    --tthread $tthread \
    --scheduler $scheduler \
    --totalEvents $totalEvents \
    --checkpoint_interval $checkpointInterval \
    --multiple_ratio $Ratio_of_Multiple_State_Access \
    --key_skewness $key_skewness \
    --overlap_ratio $overlap_ratio \
    --abort_ratio $abort_ratio \
    --CCOption $CCOption \
    --complexity $complexity \
    --isCyclic $isCyclic \
    --rootFilePath $rootFilePath
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  for scheduler in OP_NS_A TStream
  do
    runTStream
  done
}


function patEvluation() {
  CCOption=4 #SSTORE
  runTStream
}

function overview() {
  ResetParameters
  baselineEvaluation

  ResetParameters
  patEvluation
}

overview
