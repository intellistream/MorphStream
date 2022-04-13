#!/bin/bash

function ResetParameters() {
  app="TollProcessing"
  checkpointInterval=20480
  tthread=24
  scheduler="OG_DFS_A"
  CCOption=3 #TSTREAM
  complexity=10000
  NUM_ITEMS=495120
  isCyclic=0
  isGroup=0
  groupNum=1
  skewGroup="0,100"
  abort_ratio=0
  key_skewness=20
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
          --abort_ratio $abort_ratio \
          --key_skewness $key_skewness"
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
    --abort_ratio abort_ratio \
     --key_skewness $key_skewness
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  for key_skewness in 0 20 40 60 80 100
  do
    for scheduler in  OP_BFS_A OP_NS_A OG_BFS_A OG_NS_A
      do
          runTStream
      done
  done
  ResetParameters
}


function runTP() { # multi-batch exp
 ResetParameters
 app=TollProcessing
 baselineEvaluation
}
runTP
ResetParameters