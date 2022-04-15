#!/bin/bash

function ResetParameters() {
  app="StreamLedger"
  NUM_ITEMS=12288
  NUM_ACCESS=2
  checkpointInterval=10240
  tthread=24
  scheduler="BFS"
  Ratio_of_Multiple_State_Access=100
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
          --multiple_ratio $Ratio_of_Multiple_State_Access \
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

# run basic experiment for different algorithms
function baselineEvaluation() {
  for scheduler in OG_NS OP_NS
  do
    runTStream
  done
}

# run basic experiment for different algorithms
function withAbortEvaluation() {
  for scheduler in OG_NS_A OP_NS_A
  do
    runTStream
  done
}

function granularity_study() {
  # num of TD
  ResetParameters
  NUM_ACCESS=2 # OC level and OP level has similar performance before
  for app in GrepSum
  do
    # for tthread in 24
    for isCyclic in 0
    do
      for checkpointInterval in 5120 10240 20480 40960 81920
      do
        withAbortEvaluation
      done
    done
  done

  #isCyclic
  ResetParameters
    checkpointInterval=40960
    for app in GrepSum
     do
       for isCyclic in 1
       do
           withAbortEvaluation
       done
     done
    #isCyclic
    ResetParameters
      checkpointInterval=40960
      NUM_ACCESS=2
      for app in GrepSum
       do
         for isCyclic in 0 1
         do
             withAbortEvaluation
         done
       done
}
rm -rf /home/shuhao/jjzhao/data
granularity_study
ResetParameters
cd ../draw || exit

echo "newmodel/python model_granularity_batch.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
ResetParameters
echo "newmodel/python model_granularity_cyclic.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b 40960 -c $isCyclic -m $complexity"
python newmodel/model_granularity_cyclic.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b 40960 -c $isCyclic -m $complexity
ResetParameters
echo "newmodel/python model_granularity_cyclic.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n 2 -k $key_skewness -o $overlap_ratio -a $abort_ratio -b 40960 -c $isCyclic -m $complexity"
python newmodel/model_granularity_cyclic.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n 2 -k $key_skewness -o $overlap_ratio -a $abort_ratio -b 40960 -c $isCyclic -m $complexity
