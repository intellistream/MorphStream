#!/bin/bash

function ResetParameters() {
  NUM_ITEMS=115200
  checkpointInterval=10240
  tthread=24
  scheduler="BFS"
  deposit_ratio=0
  key_skewness=0
  overlap_ratio=10
  abort_ratio=0
  CCOption=3 #TSTREAM
}

function runTStream() {
  totalEvents=`expr $checkpointInterval \* $tthread`
  # NUM_ITEMS=`expr $totalEvents`
  echo "java -Xms100g -Xmx100g -jar -d64 application-0.0.2-jar-with-dependencies.jar \
          --NUM_ITEMS $NUM_ITEMS \
          --tthread $tthread \
          --scheduler $scheduler \
          --totalEvents $totalEvents \
          --checkpoint_interval $checkpointInterval \
          --deposit_ratio $deposit_ratio \
          --key_skewness $key_skewness \
          --overlap_ratio $overlap_ratio \
          --abort_ratio $abort_ratio
          --CCOption $CCOption"
  java -Xms100g -Xmx100g -Xss10M -jar -d64 application-0.0.2-jar-with-dependencies.jar \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --totalEvents $totalEvents \
    --checkpoint_interval $checkpointInterval \
    --deposit_ratio $deposit_ratio \
    --key_skewness $key_skewness \
    --overlap_ratio $overlap_ratio \
    --abort_ratio $abort_ratio \
    --CCOption $CCOption
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  # for tthread in 1 2 4 8 16 24
  # for tthread in 16
  # for deposit_ratio in 0 25 50 75 100
  # for key_skewness in 0 25 50 #0 25 50
  for overlap_ratio in 0 #0 25 50
  do
    for scheduler in BFS DFS GS OPBFS OPDFS OPGS
    # for scheduler in GS
    do
        for checkpointInterval in 1024 2048 4096 8192 10240
        do
            runTStream
        done
    done
  done
}

# run basic experiment for different algorithms
function withAbortEvaluation() {
  # for tthread in 1 2 4 8 16 24
  # for tthread in 1
  # for deposit_ratio in 0 25 50 75 100
  # for key_skewness in 0 25 50 #0 25 50
  for overlap_ratio in 0 #0 25 50
  do
    # for scheduler in BFSA DFSA GSA OPBFSA OPDFSA OPGSA
    for scheduler in BFSA
    do
        # for checkpointInterval in 1024 2048 4096 8192 10240
        for checkpointInterval in 10240
        do
            runTStream
        done
    done
  done
}

function patEvluation() {
  CCOption=4 #SSTORE
  for checkpointInterval in 1024 2048 4096 8192 10240
  do
    runTStream
  done
}


function sensitivity() {
  # TODO: more data generator properties are to be exposed
  ResetParameters
  for tthread in 1 2 4 8 16 24
  do
    baselineEvaluation
  done

  ResetParameters
  for tthread in 1 2 4 8 16 24
  do
    withAbortEvaluation
  done


  ResetParameters
  for tthread in 1 2 4 8 16 24
  do
    patEvluation
  done
}


function abortHandling() {
  ResetParameters
  # for abort_ratio in 1 10 100
  for abort_ratio in 100
  do
    # for tthread in 1 2 4 8 16 24
    for tthread in 24
    do
      withAbortEvaluation
    done
  done
}

abortHandling