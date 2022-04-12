#!/bin/bash

function ResetParameters() {
  app="TollProcessing"
  checkpointInterval=20480
  tthread=24
  scheduler="OP_BFS_A"
  CCOption=3 #TSTREAM
  complexity=10000
  NUM_ITEMS=495120
  isCyclic=0
  isGroup=0
  groupNum=1
  SchedulersForGroup="OG_BFS_A,OG_NS";
  skewGroup="0,100"
  rootFilePath="/home/shuhao/jjzhao/data"
}

function runTStream() {
  totalEvents=`expr $checkpointInterval \* $tthread`
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
          --isGroup $isGroup \
          --totalEvents $totalEvents \
          --groupNum $groupNum \
          --skewGroup $skewGroup \
          --SchedulersForGroup $SchedulersForGroup"
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
    --isGroup $isGroup \
    --totalEvents $totalEvents \
    --groupNum $groupNum \
    --skewGroup $skewGroup \
    --SchedulersForGroup $SchedulersForGroup
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  isGroup=1
  groupNum=2
  runTStream
  ResetParameters
  for scheduler in TStream OG_BFS_A OG_NS
  do
      runTStream
  done
}


function patEvluation() {
  isGroup=0
  CCOption=4 #SSTORE
  runTStream
}


function group_runner() { # multi-batch exp
 ResetParameters
 app=TollProcessing
 baselineEvaluation
 ResetParameters
 patEvluation
}
group_runner
ResetParameters