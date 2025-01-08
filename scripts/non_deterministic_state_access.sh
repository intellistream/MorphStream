#!/bin/bash

function ResetParameters() {
  app="NonGrepSum"
  checkpointInterval=10240
  tthread=24
  scheduler="OP_BFS"
  defaultScheduler="OP_BFS"
  CCOption=3 #TSTREAM
  complexity=10000
  NUM_ITEMS=12288
  nondeterministic_ratio=95
  key_skewness=0
  number_access=1


  isCyclic=1
  isDynamic=1
  workloadType="default"
  schedulerPool="OP_BFS"
  rootFilePath="/home/jjzhao/Benchmark/MorphStream"
  shiftRate=1
  totalEvents=`expr $checkpointInterval \* $tthread \* 1 \* $shiftRate`
}

function runTStream() {
  echo "java -Xms300g -Xmx300g -jar -d64 /home/jjzhao/project/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --tthread $tthread \
          --scheduler $scheduler \
          --defaultScheduler $defaultScheduler \
          --checkpoint_interval $checkpointInterval \
          --CCOption $CCOption \
          --complexity $complexity \
          --nondeterministic_ratio $nondeterministic_ratio \
          --key_skewness $key_skewness \
          --NUM_ACCESS $number_access \
          --isCyclic $isCyclic \
          --rootFilePath $rootFilePath \
          --isDynamic $isDynamic \
          --totalEvents $totalEvents \
          --shiftRate $shiftRate \
          --workloadType $workloadType \
          --schedulerPool $schedulerPool"
  java -Xms64g -Xmx64g -Xss100M -XX:+PrintGCDetails -Xmn64g -XX:+UseG1GC -jar -d64 /home/jjzhao/project/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --defaultScheduler $defaultScheduler \
    --checkpoint_interval $checkpointInterval \
    --CCOption $CCOption \
    --complexity $complexity \
    --nondeterministic_ratio $nondeterministic_ratio \
    --key_skewness $key_skewness \
    --NUM_ACCESS $number_access \
    --isCyclic $isCyclic \
    --rootFilePath $rootFilePath \
    --isDynamic $isDynamic \
    --totalEvents $totalEvents \
    --shiftRate $shiftRate \
    --workloadType $workloadType \
    --schedulerPool $schedulerPool
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  runTStream

  scheduler=TStream
  runTStream
}


function patEvluation() {
  CCOption=4 #SSTORE
  runTStream
}


function runner() { # multi-batch exp
 ResetParameters
 app=NonGrepSum
 for nondeterministic_ratio in 0 10
 do
 baselineEvaluation
 #patEvluation
 done

}
runner
ResetParameters