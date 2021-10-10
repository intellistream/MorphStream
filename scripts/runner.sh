#!/bin/bash

function ResetParameters() {
  NUM_ITEMS=5000000
  totalEvents=983040
  tthread=24
  scheduler="BFS"
  deposit_ratio=25
  key_skewness=0
  overlap_ratio=10
}

function runTStream() {
  echo "java -Xms60g -Xmx60g -jar -d64 application-0.0.2-jar-with-dependencies.jar \
          --NUM_ITEMS $NUM_ITEMS \
          --tthread $tthread \
          --scheduler $scheduler \
          --totalEvents $totalEvents \
          --checkpoint_interval $checkpointInterval \
          --deposit_ratio $deposit_ratio \
          --key_skewness $key_skewness \
          --overlap_ratio $overlap_ratio"
  totalEvents=`expr $checkpointInterval \* $tthread`
  java -Xms60g -Xmx60g -jar -d64 application-0.0.2-jar-with-dependencies.jar \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --totalEvents $totalEvents \
    --checkpoint_interval $checkpointInterval \
    --deposit_ratio $deposit_ratio \
    --key_skewness $key_skewness \
    --overlap_ratio $overlap_ratio
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  ResetParameters
  for tthread in 1 2 4 8 16 24
  do
    for scheduler in BFS DFS GS OPBFS OPDFS OPGS
    do
        for checkpointInterval in 2048
        do
            runTStream
        done
    done
  done
}

# run basic experiment for different algorithms
function withAbortEvaluation() {
  ResetParameters
  for tthread in 1 2 4 8 16 24
  do
    for scheduler in BFSA DFSA GSA OPBFSA OPDFSA OPGSA
    do
        for checkpointInterval in 2048
        do
            runTStream
        done
    done
  done
}


# TODO: more data generator properties are to be exposed

baselineEvaluation
withAbortEvaluation