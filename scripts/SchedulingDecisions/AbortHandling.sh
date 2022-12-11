#!/bin/bash

function ResetParameters() {
  app="StreamLedger"
  NUM_ITEMS=12288
  NUM_ACCESS=2
  checkpointInterval=10240
  tthread=24
  scheduler="OP_NS"
  multiple_ratio=100
  key_skewness=0
  overlap_ratio=0
  abort_ratio=0
  CCOption=3 #TSTREAM
  complexity=0
  isCyclic=1
  rootFilePath="${project_Dir}/result/data/AbortHandling"
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
          --multiple_ratio $Ratio_of_Multiple_State_Access \
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

function varying_computation_complexity() {
   ResetParameters
   abort_ratio=5000
     for app in GrepSum
     do
         for complexity in 0 10000 20000 40000 60000 80000 100000
         do
           for scheduler in OP_NS OP_NS_A
           do
             runTStream
           done
         done
       done
}

function varying_abort_ratio() {
  ResetParameters
    for app in  GrepSum
    do
        for abort_ratio in 0 1000 2000 5000 7000 9000
        do
          for scheduler in OP_NS OP_NS_A
          do
            runTStream
          done
        done
      done
}

function abort_mechanism_study() {
 varying_computation_complexity
 varying_abort_ratio
}

rm -rf /home/shuhao/jjzhao/data
abort_mechanism_study
ResetParameters
cd draw || exit

echo "newmodel/python model_abort_abortRatio.py -i $NUM_ITEMS -d Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
python newmodel/model_abort_abortRatio.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
ResetParameters
echo "newmodel/python model_abort_complexity.py -i $NUM_ITEMS -d Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
python newmodel/model_abort_complexity.py -i $NUM_ITEMS -d $Ratio_of_Multiple_State_Access -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
