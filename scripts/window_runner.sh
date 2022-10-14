#!/bin/bash

function ResetParameters() {
  app="WindowedGrepSum"
  NUM_ITEMS=1024
  NUM_ACCESS=1
  checkpointInterval=102400
  tthread=16
  scheduler="OP_NS"
  deposit_ratio=25
  key_skewness=0
  overlap_ratio=0
  window_trigger_period=100
  window_size=1024
  CCOption=3 #TSTREAM
  complexity=10000
  txn_length=100
  isCyclic=0
  rootFilePath="/home/myc/data"
}

function runTStream() {
  totalEvents=`expr $checkpointInterval \* $tthread`
  # NUM_ITEMS=`expr $totalEvents`
  echo "java -Xms16g -Xmx16g -jar -d64 application-0.0.2-jar-with-dependencies.jar \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --NUM_ACCESS $NUM_ACCESS \
          --tthread $tthread \
          --scheduler $scheduler \
          --totalEvents $totalEvents \
          --checkpoint_interval $checkpointInterval \
          --deposit_ratio $deposit_ratio \
          --key_skewness $key_skewness \
          --overlap_ratio $overlap_ratio \
          --window_trigger_period $window_trigger_period \
          --window_size $window_size \
          --CCOption $CCOption \
          --complexity $complexity \
          --isCyclic $isCyclic \
          --txn_length $txn_length \
          --rootFilePath $rootFilePath"
  java -Xms16g -Xmx16g -Xss100M -jar -d64 application-0.0.2-jar-with-dependencies.jar \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --NUM_ACCESS $NUM_ACCESS \
    --tthread $tthread \
    --scheduler $scheduler \
    --totalEvents $totalEvents \
    --checkpoint_interval $checkpointInterval \
    --deposit_ratio $deposit_ratio \
    --key_skewness $key_skewness \
    --overlap_ratio $overlap_ratio \
    --window_trigger_period $window_trigger_period \
    --window_size $window_size \
    --CCOption $CCOption \
    --complexity $complexity \
    --isCyclic $isCyclic \
    --txn_length $txn_length \
    --rootFilePath $rootFilePath
}



#ResetParameters
#for window_size in 1000 10000 100000
#do
#  runTStream
#done

#ResetParameters
#for window_trigger_period in 100 1000 10000
#do
#  runTStream
#done

ResetParameters
cd draw/window || exit
#python window_size.py -i $NUM_ITEMS -b $checkpointInterval -t $txn_length -d $tthread -o $window_trigger_period -a $window_size
python window_trigger_period.py -i $NUM_ITEMS -b $checkpointInterval -t $txn_length -d $tthread -o $window_trigger_period -a $window_size