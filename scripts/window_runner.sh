#!/bin/bash

function ResetParameters() {
  app="WindowedGrepSum"
  NUM_ITEMS=122880
  NUM_ACCESS=2
  checkpointInterval=10240
  tthread=24
  scheduler="OP_NS"
  deposit_ratio=25
  key_skewness=0
  overlap_ratio=0
  abort_ratio=0
  window_ratio=1
  window_size=1024
  CCOption=3 #TSTREAM
  complexity=10000
  isCyclic=1
}

function runTStream() {
  totalEvents=`expr $checkpointInterval \* $tthread`
  # NUM_ITEMS=`expr $totalEvents`
  echo "java -Xms100g -Xmx100g -jar -d64 application-0.0.2-jar-with-dependencies.jar \
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
          --window_ratio $window_ratio \
          --window_size $window_size \
          --CCOption $CCOption \
          --complexity $complexity \
          --isCyclic $isCyclic"
  java -Xms100g -Xmx100g -Xss100M -jar -d64 application-0.0.2-jar-with-dependencies.jar \
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
    --window_ratio $window_ratio \
    --window_size $window_size \
    --CCOption $CCOption \
    --complexity $complexity \
    --isCyclic $isCyclic
}


runTStream