#!/bin/bash

function ResetParameters() {
  app="StreamLedger"
  NUM_ITEMS=12288
  NUM_ACCESS=10
  checkpointInterval=10240
  tthread=24
  scheduler="BFS"
  deposit_ratio=25
  key_skewness=0
  overlap_ratio=0
  abort_ratio=0
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
  for scheduler in GS OPGS
  # for scheduler in GS
  do
    runTStream
  done
}

# run basic experiment for different algorithms
function withAbortEvaluation() {
  # for scheduler in BFSA DFSA GSA OPBFSA OPDFSA OPGSA
  for scheduler in GSA OPGSA
  do
    runTStream
  done
}

function granularity_study() {
  
  # Num of FD
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

  # # Average num of OP in OC
  # ResetParameters
  # NUM_ACCESS=1 # OC level and OP level has similar performance before
  # for app in StreamLedger GrepSum
  # do
  #   # for tthread in 24
  #   for isCyclic in 0 1
  #   do
  #     for NUM_ITEMS in 12288 122880 1228800
  #     do
  #       for checkpointInterval in 5120 10240 20480 40960 81920
  #       do
  #         withAbortEvaluation
  #       done
  #     done
  #   done
  # done

  # Average num of OP in OC
  ResetParameters
  NUM_ACCESS=1 # OC level and OP level has similar performance before
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

  # # Abort Ratio, rollback overhead.
  # ResetParameters
  # NUM_ACCESS=1 # OC level and OP level has similar performance before
  # for app in StreamLedger GrepSum
  # do
  #   # for tthread in 24
  #   for isCyclic in 0 1
  #   do
  #     for abort_ratio in 0 1 10 100 1000 2000 5000
  #     do
  #       withAbortEvaluation
  #     done
  #   done
  # done

  
  # Write Only Ratio
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
}

function abort_mechanism_study() {
  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for abort_ratio in 0 1000 2000 5000 7000 9000
      do
        for scheduler in OPGS OPGSA
        do
          runTStream
        done
      done
    done
  done

  # ResetParameters
  # for app in StreamLedger GrepSum
  # do
  #   # for tthread in 24
  #   for isCyclic in 1
  #   do
  #     for abort_ratio in 0 1000 2000 5000 7000 9000
  #     do
  #       withAbortEvaluation
  #     done
  #   done
  # done


  # # Average num of OP in OC
  ResetParameters
  abort_ratio=5000
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for checkpointInterval in 5120 10240 20480 40960 81920
      do
        for scheduler in OPGS OPGSA
        do
          runTStream
        done
      done
    done
  done
  # ResetParameters
  # abort_ratio=5000
  # for app in StreamLedger GrepSum
  # do
  #   # for tthread in 24
  #   for isCyclic in 1
  #   do
  #     for checkpointInterval in 5120 10240 20480 40960
  #     do
  #       withAbortEvaluation
  #     done
  #   done
  # done

  # Complexity of OP process
  ResetParameters
  abort_ratio=5000
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for complexity in 0 10000 20000 40000 60000 80000 100000
      do
        for scheduler in OPGS OPGSA
        do
          runTStream
        done
      done
    done
  done
  # ResetParameters
  # abort_ratio=5000
  # for app in StreamLedger GrepSum
  # do
  #   # for tthread in 24
  #   for isCyclic in 0 1
  #   do
  #     for complexity in 10000 20000 40000 60000 80000 100000
  #     do
  #       withAbortEvaluation
  #     done
  #   done
  # done

  # Write Only Ratio
  ResetParameters
  abort_ratio=5000
  for app in StreamLedger
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for deposit_ratio in 0 25 50 75 100
      do
        for scheduler in OPGS OPGSA
        do
          runTStream
        done
      done
    done
  done
  # # Write Only Ratio
  # ResetParameters
  # abort_ratio=5000
  # for app in StreamLedger
  # do
  #   # for tthread in 24
  #   for isCyclic in 1
  #   do
  #     for deposit_ratio in 0 25 50 75 100
  #     do
  #       withAbortEvaluation
  #     done
  #   done
  # done

  # Num of FD
  ResetParameters
  abort_ratio=5000
  for app in GrepSum
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for NUM_ACCESS in 1 2 4 6 8 10
      do
        for scheduler in OPGS OPGSA
        do
          runTStream
        done
      done
    done
  done
  # ResetParameters
  # abort_ratio=5000
  # for app in GrepSum
  # do
  #   # for tthread in 24
  #   for isCyclic in 1
  #   do
  #     for NUM_ACCESS in 1 2 4 6 8 10
  #     do
  #       withAbortEvaluation
  #     done
  #   done
  # done
}

function exploration_strategy_study() {
# Average num of OP in OC
  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for complexity in 0 10000 20000 40000 60000 80000 100000
      do
        for scheduler in OPDFS OPBFS OPGS
        do
          runTStream
        done
      done
    done
  done

  # Average num of OP in OC
  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for NUM_ITEMS in 12288 122880 1228800
      do
        for scheduler in OPDFS OPBFS OPGS
        do
          runTStream
        done
      done
    done
  done

  # Average num of OP in OC
  ResetParameters
  for app in StreamLedger GrepSum
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for checkpointInterval in 5120 10240 20480 40960 81920
      do
        for scheduler in OPDFS OPBFS OPGS
        do
          runTStream
        done
      done
    done
  done

  # Write Only Ratio
  ResetParameters
  for app in StreamLedger
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for deposit_ratio in 0 25 50 75 100
      do
        for scheduler in OPDFS OPBFS OPGS
        do
          runTStream
        done
      done
    done
  done

  # Num of FD
  ResetParameters
  for app in GrepSum
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for NUM_ACCESS in 1 2 4 6 8 10
      do
        for scheduler in OPDFS OPBFS OPGS
        do
          runTStream
        done
      done
    done
  done

  # key_skewness
  ResetParameters
  for app in GrepSum StreamLedger
  do
    # for tthread in 24
    for isCyclic in 1
    do
      for key_skewness in 0 25 50 75 100
      do
        for scheduler in OPDFS OPBFS OPGS
        do
          runTStream
        done
      done
    done
  done
}

# Granularity selection
granularity_study
# Exploration Strategy
exploration_strategy_study
# Abort mechanism selection
abort_mechanism_study

cd draw || exit

ResetParameters
echo "python model_granularity_cyclic.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_granularity_cyclic.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

NUM_ACCESS=1 # where OP and OC has the similar performance under default setting
# cd draw || exit
for isCyclic in 0 1
do
  echo "python model_granularity_access.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_granularity_access.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  echo "python model_granularity_batch.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_granularity_batch.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  echo "python model_granularity_writeonly.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_granularity_writeonly.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  # echo "python model_granularity_key.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  # python model_granularity_key.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  # echo "python model_granularity_abort.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  # python model_granularity_abort.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
done


ResetParameters
abort_ratio=0
# cd draw || exit
for isCyclic in 1
do
  echo "python model_exploration_strategy_key.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_exploration_strategy_key.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  echo "python model_exploration_strategy_batch.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_exploration_strategy_batch.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  echo "python model_exploration_strategy_access.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_exploration_strategy_access.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  echo "python model_exploration_strategy_writeonly.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_exploration_strategy_writeonly.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  echo "python model_exploration_strategy_skewness.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_exploration_strategy_skewness.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  echo "python model_exploration_strategy_complexity.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_exploration_strategy_complexity.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
done


ResetParameters
abort_ratio=5000
# cd draw || exit
for isCyclic in 1
do
  echo "python model_abort_mechanism_abort.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_abort_mechanism_abort.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  echo "python model_abort_mechanism_complexity.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_abort_mechanism_complexity.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  echo "python model_abort_mechanism_batch.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_abort_mechanism_batch.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  echo "python model_abort_mechanism_access.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_abort_mechanism_access.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity

  echo "python model_abort_mechanism_writeonly.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity"
  python model_abort_mechanism_writeonly.py -i $NUM_ITEMS -n $NUM_ACCESS -k $key_skewness -o $overlap_ratio -a $abort_ratio -b $checkpointInterval -c $isCyclic -m $complexity
done