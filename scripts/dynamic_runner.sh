#!/bin/bash

function ResetParameters() {
  app="StreamLedger"
  checkpointInterval=10240
  tthread=24
  scheduler="OP_BFS_A"
  CCOption=3 #TSTREAM
  complexity=10000
  NUM_ITEMS=122880
  isCyclic=0
  isDynamic=0
  rootFilePath="/home/shuhao/jjzhao/data"
  shiftRate=2
  totalEvents=`expr $checkpointInterval \* $tthread \* 8 \*$shiftRate`
}

function runTStream() {
  echo "java -Xms100g -Xmx100g -jar -d64 /home/shuhao/jjzhao/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --tthread $tthread \
          --scheduler $scheduler \
          --checkpoint_interval $checkpointInterval \
          --CCOption $CCOption \
          --complexity $complexity \
          --isCyclic $isCyclic \
          --rootFilePath $rootFilePath \
          --isDynamic $isDynamic \
          --totalEvents $totalEvents \
          --shiftRate $shiftRate"
  java -Xms100g -Xmx100g -Xss100M -jar -d64 /home/shuhao/jjzhao/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --checkpoint_interval $checkpointInterval \
    --CCOption $CCOption \
    --complexity $complexity \
    --isCyclic $isCyclic \
    --rootFilePath $rootFilePath \
    --isDynamic $isDynamic \
    --totalEvents $totalEvents \
    --shiftRate $shiftRate
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  isDynamic=1
  runTStream
  ResetParameters

  scheduler=TStream
  isDynamic=0
  runTStream
}


function patEvluation() {
  isDynamic=0
  CCOption=4 #SSTORE
  runTStream
}


function dynamic_runner() { # multi-batch exp
 ResetParameters
 app=StreamLedger
 baselineEvaluation
 patEvluation
}
dynamic_runner
ResetParameters