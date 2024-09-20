#!/bin/bash
source ../global.sh || exit
function ResetParameters() {
  app="StreamLedger"
  NUM_ITEMS=12288
  NUM_ACCESS=2
  checkpointInterval=10240
  tthread=24
  scheduler="BFS"
  multiple_ratio=100
  key_skewness=0
  overlap_ratio=0
  abort_ratio=0
  CCOption=3 #TSTREAM
  complexity=0
  isCyclic=0
  rootFilePath="${project_Dir}/result/data/ExplorationStrategies"
}

function runTStream() {
  totalEvents=`expr $checkpointInterval \* $tthread`
  # NUM_ITEMS=`expr $totalEvents`
  echo "java -Xms100g -Xmx100g -jar -d64 ${jar_Dir} \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --NUM_ACCESS $NUM_ACCESS \
          --tthread $tthread \
          --scheduler $scheduler \
          --totalEvents $totalEvents \
          --checkpoint_interval $checkpointInterval \
          --multiple_ratio $Ratio_of_Multiple_State_Access\
          --key_skewness $key_skewness \
          --overlap_ratio $overlap_ratio \
          --abort_ratio $abort_ratio \
          --CCOption $CCOption \
          --complexity $complexity \
           --isCyclic $isCyclic \
           --rootFilePath $rootFilePath"
  java -Xms100g -Xmx100g -Xss100M -jar -d64 $jar_Dir \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --NUM_ACCESS $NUM_ACCESS \
    --tthread $tthread \
    --scheduler $scheduler \
    --totalEvents $totalEvents \
    --checkpoint_interval $checkpointInterval \
    --multiple_ratio $Ratio_of_Multiple_State_Access \
    --key_skewness $key_skewness \
    --overlap_ratio $overlap_ratio \
    --abort_ratio $abort_ratio \
    --CCOption $CCOption \
    --complexity $complexity \
    --isCyclic $isCyclic \
    --rootFilePath $rootFilePath
}

function varying_punctuation_interval() {
  ResetParameters
    for app in GrepSum
    do
      for key_skewness in 0 75
      do
        for checkpointInterval in 5120 10240 20480 40960 81920
        do
          for scheduler in OP_BFS OP_DFS OP_NS
          do
            runTStream
          done
        done
      done
    done
  ResetParameters
  cd ../draw || exit
  key_skewness=0
  echo "newmodel/python model_exploration_strategy_batch.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python newmodel/model_exploration_strategy_batch.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
  ResetParameters
  key_skewness=75
  echo "newmodel/python model_exploration_strategy_batch.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python newmodel/model_exploration_strategy_batch.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
}

function vary_skew(){
   ResetParameters
   checkpointInterval=81920
   for app in GrepSum
    do
      for key_skewness in 0 25 50 75 100
        do
          for scheduler in OP_BFS OP_DFS OP_NS OG_BFS OG_DFS OG_NS
          do
            runTStream
          done
        done
   done
  ResetParameters
}

function exploration_strategy_study() {
  varying_punctuation_interval
  vary_skew
}

exploration_strategy_study

