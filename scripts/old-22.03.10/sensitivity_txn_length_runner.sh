#!/bin/bash
# add this one for transaction length exp, will merge to sensitivity in the future

function ResetParameters() {
  app="GrepSum"
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
  txn_length=1
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
          --txn_length $txn_length \
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
    --txn_length $txn_length \
    --isCyclic $isCyclic
}
# run basic experiment for different algorithms
function baselineEvaluation() {
  # for scheduler in BFS DFS GS OPBFS OPDFS OPGS TStream
  for scheduler in TStream
  # for scheduler in GS
  do
    runTStream
  done
}

# run basic experiment for different algorithms
function withAbortEvaluation() {
  # for scheduler in BFSA DFSA GSA OPBFSA OPDFSA OPGSA
  for scheduler in OPGSA
  do
    runTStream
  done
}

function patEvluation() {
  CCOption=4 #SSTORE
  runTStream
}


# checkpointInterval
function sensitivity_study_batch() {
  ResetParameters
  # for tthread in 1 2 4 8 16 24 48
  for app in StreamLedger GrepSum
  do
    for isCyclic in 0 1
    do
      for checkpointInterval in 5120 10240 20480 40960 81920
      do
        baselineEvaluation
      done
    done
  done

  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for checkpointInterval in 5120 10240 20480 40960 81920
      do
        withAbortEvaluation
      done
    done
  done


  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for checkpointInterval in 5120 10240 20480 40960 81920
      do
        patEvluation
      done
    done
  done
}

# key_skewness
function sensitivity_study_skewness() {
  # for tthread in 1 2 4 8 16 24 48
  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for key_skewness in 0 25 50 75 100
      do
        baselineEvaluation
      done
    done
  done

  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for key_skewness in 0 25 50 75 100
      do
        withAbortEvaluation
      done
    done
  done

  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for key_skewness in 0 25 50 75 100
      do
        patEvluation
      done
    done
  done
}

# abort_ratio
function sensitivity_study_abort() {
  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for abort_ratio in 0 1 10 100 1000
      do
        baselineEvaluation
      done
    done
  done

  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for abort_ratio in 0 1 10 100 1000
      do
        withAbortEvaluation
      done
    done
  done


  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for abort_ratio in 0 1 10 100 1000
      do
        patEvluation
      done
    done
  done
}

# NUM_ACCESS
function sensitivity_study_access() {
  ResetParameters
  for app in GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for NUM_ACCESS in 1 2 4 6 8 10
      do
        baselineEvaluation
      done
    done
  done

  ResetParameters
  for app in GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for NUM_ACCESS in 1 2 4 6 8 10
      do
        withAbortEvaluation
      done
    done
  done


  ResetParameters
  for app in GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for NUM_ACCESS in 1 2 4 6 8 10
      do
        patEvluation
      done
    done
  done
}

# writeonly
function sensitivity_study_writeonly() {
  # for tthread in 1 2 4 8 16 24 48
  ResetParameters
  for app in StreamLedger
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for deposit_ratio in 0 25 50 75 100
      do
        baselineEvaluation
      done
    done
  done

  ResetParameters
  for app in StreamLedger 
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for deposit_ratio in 0 25 50 75 100
      do
        withAbortEvaluation
      done
    done
  done

  ResetParameters
  for app in StreamLedger 
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for deposit_ratio in 0 25 50 75 100
      do
        patEvluation
      done
    done
  done
}

# NUM_ACCESS
function sensitivity_study_keys() {
  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for NUM_ITEMS in 12288 122880 1228800
      do
        baselineEvaluation
      done
    done
  done

  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for NUM_ITEMS in 12288 122880 1228800
      do
        withAbortEvaluation
      done
    done
  done


  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 0 1
    do
      for NUM_ITEMS in 12288 122880 1228800
      do
        patEvluation
      done
    done
  done
}


# NUM_ACCESS
function sensitivity_study_length() {
  ResetParameters
  for app in GrepSum
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for txn_length in 1 2 4 6 8 10
      do
        baselineEvaluation
      done
    done
  done

  ResetParameters
  for app in GrepSum
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for txn_length in 1 2 4 6 8 10
      do
        withAbortEvaluation
      done
    done
  done

  ResetParameters
    for app in GrepSum
    do
      # for tthread in 24
      for isCyclic in 1
      do
        for txn_length in 1 2 4 6 8 10
        do
          patEvluation
        done
      done
    done
}

sensitivity_study_length

# draw
ResetParameters
cd draw || exit
for isCyclic in 1
do
  echo "python sensitivity_length.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python sensitivity_length.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
done