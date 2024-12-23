#!/bin/bash

function ResetParameters() {
  app="WindowedGrepSum"
  NUM_ITEMS=12288
  NUM_ACCESS=2
  checkpointInterval=10240
  tthread=24
  scheduler="OP_NS"
  deposit_ratio=25
  key_skewness=0
  overlap_ratio=0
  window_trigger_period=1024
  window_size=1024
  CCOption=3 #TSTREAM
  complexity=10000
  isCyclic=0
  rootFilePath="/home/myc/data"
}

function runTStream() {
  totalEvents=`expr $checkpointInterval \* $tthread \* 10`
  # NUM_ITEMS=`expr $totalEvents`
  echo "java -Xms16g -Xmx16g -jar -d64 application-0.0.2-jar-with-dependencies.jar \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --NUM_ACCESS $NUM_ACCESS \
          --tthread $tthread \
          --scheduler $scheduler \
          --totalEvents $totalEvents \
          --checkpoint_interval $checkpointInterval \
          --deposit_ratio $deposit_ratio \
          --key_skewness $key_skewness \
          --overlap_ratio $overlap_ratio \
          --window_trigger_period $window_trigger_period \
          --window_size $window_size \
          --CCOption $CCOption \
          --complexity $complexity \
          --isCyclic $isCyclic
          --rootFilePath $rootFilePath"
  java -Xms16g -Xmx16g -Xss100M -jar -d64 application-0.0.2-jar-with-dependencies.jar \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --NUM_ACCESS $NUM_ACCESS \
    --tthread $tthread \
    --scheduler $scheduler \
    --totalEvents $totalEvents \
    --checkpoint_interval $checkpointInterval \
    --deposit_ratio $deposit_ratio \
    --key_skewness $key_skewness \
    --overlap_ratio $overlap_ratio \
    --window_trigger_period $window_trigger_period \
    --window_size $window_size \
    --CCOption $CCOption \
    --complexity $complexity \
    --isCyclic $isCyclic \
    --rootFilePath $rootFilePath
}


# run basic experiment for different algorithms
function baselineEvaluation() {
  for scheduler in OP_NS TStream
  do
    runTStream
  done
}


function patEvluation() {
  CCOption=4 #SSTORE
  runTStream
}

ResetParameters
for window_size in 1000 10000 100000
do
  baselineEvaluation
  patEvluation
done

ResetParameters
for window_trigger_period in 100 1000 10000 100000
do
  baselineEvaluation
  patEvluation
done