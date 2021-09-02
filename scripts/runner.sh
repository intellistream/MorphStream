#!/bin/bash

function ResetParameters() {
  NUM_ITEMS=5000000
  numberOfBatches=1
  events=983040
  fanoutDist="zipfcenter"
  idGenType="normal"
  tthread=24
  scheduler="BFS"
}

function runTStream() {
  java -Xms60g -Xmx60g -jar -d64 application-0.0.2-jar-with-dependencies.jar \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --totalEventsPerBatch $events \
    --numberOfBatches $numberOfBatches \
    --fanoutDist $fanoutDist  \
    --idGenType $idGenType \
    --scheduler $scheduler
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  ResetParameters
  for scheduler in BFS DFS GS
  do
      for events in 983040
      do
          runTStream
      done
  done
}

# run basic experiment for different algorithms
function withAbortEvaluation() {
  ResetParameters
  for scheduler in BFSA DFSA GSA
  do
      for events in 983040
      do
          runTStream
      done
  done
}

# TODO: more data generator properties are to be exposed

baselineEvaluation
withAbortEvaluation