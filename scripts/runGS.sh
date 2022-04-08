#!/bin/bash

function ResetParameters() {
  app="GrepSum"
  checkpointInterval=10240
  tthread=24
  scheduler="OP_BFS_A"
  CCOption=3 #TSTREAM
  complexity=10000
  NUM_ITEMS=122880
  NUM_ACCESS=10
  isCyclic=0
  isDynamic=0
  multiple_ratio=0
  rootFilePath="/home/shuhao/jjzhao/data"

  key_skewness=20
  overlap_ratio=10
  abort_ratio=0
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
          --isDynamic $isDynamic \
          --totalEvents $totalEvents \
          --multiple_ratio $multiple_ratio \
          --NUM_ACCESS $NUM_ACCESS"
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
    --multiple_ratio $multiple_ratio \
    --NUM_ACCESS $NUM_ACCESS
}

function gs_runner() { # multi-batch exp
 ResetParameters
  for scheduler in OP_NS_A OG_NS_A
   do
       for multiple_ratio in 0 10 20 30 40 50 60 70 80 90 100
               do
                 runTStream
               done
   done
}
gs_runner
ResetParameters
# draw
ResetParameters
cd draw/NewVersion || exit
echo "python model_granularity_access_gs.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_granularity_access_gs.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity