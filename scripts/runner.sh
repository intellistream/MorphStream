#!/bin/bash

function ResetParameters() {
  numberOfDLevels=1024
  numberOfBatches=1
  events=983040
  fanoutDist="zipfcenter"
  idGenType="normal"
  tt=24
  scheduler="BFS"
}

function runTStream() {
  java -Xms60g -Xmx60g -jar -d64 application-0.0.1-jar-with-dependencies.jar \
    --numberOfDLevels $numberOfDLevels \
    -tt $tt \
    --totalEventsPerBatch $events \
    --numberOfBatches $numberOfBatches \
    --fanoutDist $fanoutDist  \
    --idGenType $idGenType \
    --scheduler $scheduler
}

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

baselineEvaluation