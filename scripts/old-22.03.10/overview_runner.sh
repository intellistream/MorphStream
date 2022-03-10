#!/bin/bash

function ResetParameters() {
  app="StreamLedger"
  NUM_ITEMS=122880
  NUM_ACCESS=10
  checkpointInterval=10240
  tthread=24
  scheduler="BFS"
  deposit_ratio=25
  key_skewness=25
  overlap_ratio=0
  abort_ratio=100
  CCOption=3 #TSTREAM
  complexity=10000
  isCyclic=1
}

function runTStream() {
  totalEvents=`expr $checkpointInterval \* $tthread`
  # NUM_ITEMS=`expr $totalEvents`
  echo "java -Xms100g -Xmx100g -jar -d64 application-0.0.2-jar-with-dependencies.jar \
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
          --abort_ratio $abort_ratio \
          --CCOption $CCOption \
          --complexity $complexity \
          --isCyclic $isCyclic"
  java -Xms100g -Xmx100g -Xss100M -jar -d64 application-0.0.2-jar-with-dependencies.jar \
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
    --abort_ratio $abort_ratio \
    --CCOption $CCOption \
    --complexity $complexity \
    --isCyclic $isCyclic
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  # for scheduler in BFS DFS GS OPBFS OPDFS OPGS TStream
  for scheduler in GSA OPGSA TStream
  # for scheduler in OPGSA
  do
    runTStream
  done
}


function patEvluation() {
  CCOption=4 #SSTORE
  runTStream
}

# overview experiment scalability exp
function overview_scalability() {
  ResetParameters
  for app in StreamLedger GrepSum
  do
    for isCyclic in 0 1
    do
        for tthread in 1 4 8 12 16 20 24
        # for tthread in 24
        do
          baselineEvaluation
        done
    done
  done

  ResetParameters
  for app in StreamLedger GrepSum
  do
    for isCyclic in 0 1
    do
      for tthread in 1 4 8 12 16 20 24
      # for tthread in 24
      do
        patEvluation
      done
    done
  done
}


function overview() { # multi-batch exp
  ResetParameters
  for isCyclic in 1
  do
    for NUM_ITEMS in 12288 122880 1228800
    do
      for abort_ratio in 0 10 100 1000 5000
      do
        for key_skewness in 0 25 50 75 100
        do
          app=StreamLedger
          for deposit_ratio in 0 25 50 75 100
          do
            baselineEvaluation
          done
          app=GrepSum
          for NUM_ACCESS in 0 2 4 6 8 10
          do
            baselineEvaluation
          done
        done
      done
    done
  done

  ResetParameters
  for isCyclic in 1
  do
    for NUM_ITEMS in 12288 122880 1228800
    do
      for abort_ratio in 0 10 100 1000 5000
      do
        for key_skewness in 0 25 50 75 100
        do
          app=StreamLedger
          for deposit_ratio in 0 25 50 75 100
          do
            patEvluation
          done
          app=GrepSum
          for NUM_ACCESS in 0 2 4 6 8 10
          do
            patEvluation
          done
        done
      done
    done
  done
}


overview_scalability
ResetParameters
cd draw || exit
for isCyclic in 0 1
do
  # python overview.py
  echo "python overview_all.py -i $NUM_ITEMS -d $deposit_ratio -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python overview_all.py -i $NUM_ITEMS -d $deposit_ratio -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
  echo "python overview_all_latency.py -i $NUM_ITEMS -d $deposit_ratio -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python overview_all_latency.py -i $NUM_ITEMS -d $deposit_ratio -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
  echo "python sensitivity_cyclic.py -i $NUM_ITEMS -d $deposit_ratio -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python sensitivity_cyclic.py -i $NUM_ITEMS -d $deposit_ratio -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
  echo "python overview_breakdown.py -i $NUM_ITEMS -d $deposit_ratio -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python overview_breakdown.py -i $NUM_ITEMS -d $deposit_ratio -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
done