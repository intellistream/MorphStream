#!/bin/bash

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
  rootFilePath="/home/shuhao/jjzhao/data"
}

function runTStream() {
  totalEvents=`expr $checkpointInterval \* $tthread`
  # NUM_ITEMS=`expr $totalEvents`
  echo "java -Xms100g -Xmx100g -jar -d64 /home/shuhao/jjzhao/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
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
  java -Xms100g -Xmx100g -Xss100M -jar -d64 /home/shuhao/jjzhao/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
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

function exploration_strategy_study() {
# Average num of OP in OC(LD)
  ResetParameters
  for app in GrepSum
  do
    for isCyclic in 0
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
   checkpoint_interval=40960
    for app in GrepSum
    do
      for isCyclic in 0
      do
        for key_skewness in 0 25 50 75 100
        do
          for scheduler in OP_BFS OP_DFS OP_NS OG_BFS OG_DFS OG_NS
          do
            runTStream
          done
        done
      done
    done

}

rm -rf /home/shuhao/jjzhao/data
exploration_strategy_study
ResetParameters
cd draw || exit

echo "newmodel/python model_exploration_strategy_batch.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
python newmodel/model_exploration_strategy_batch.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
ResetParameters
echo "newmodel/python model_exploration_skewness.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
python newmodel/python model_exploration_skewness.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
