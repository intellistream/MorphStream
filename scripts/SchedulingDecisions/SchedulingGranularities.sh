#!/bin/bash
source ../global.sh || exit
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
  rootFilePath="${project_Dir}/result/data/SchedulingGranularities"
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

# run basic experiment for different algorithms
function baselineEvaluation() {
  for scheduler in OG_NS OP_NS
  do
    runTStream
  done
}

function cyclic_acyclic() {
  ResetParameters
  checkpointInterval=40960
  for app in GrepSum
  do
    for isCyclic in 0 1
      do
        for scheduler in OG_NS_A OP_NS_A
          do
            runTStream
          done
      done
  done
}

function varying_punctuation_interval() {
  ResetParameters
  NUM_ACCESS=1 # OC level and OP level has similar performance before
  for app in GrepSum
  do
    for isCyclic in 0
      do
        for checkpointInterval in 5120 10240 20480 40960 81920
        do
          for scheduler in OG_NS_A OP_NS_A
            do
              runTStream
            done
        done
      done
    done
}

function varying_ratio_of_multiple_state_access() {
  ResetParameters
  checkpointInterval=40960
  NUM_ACCESS=5
  for app in GrepSum
  do
    for isCyclic in 0
      do
       for Ratio_of_Multiple_State_Access in 10 30 50 70 90
       do
          for scheduler in OG_NS_A OP_NS_A
            do
              runTStream
            done
       done
      done
  done
}

function scheduling_granularity_study() {
   cyclic_acyclic
   varying_punctuation_interval
   varying_ratio_of_multiple_state_access
}
rm -rf /home/shuhao/jjzhao/data
granularity_study
ResetParameters
