#!/bin/bash

function ResetParameters() {
  app="StreamLedger"
  checkpointInterval=10240
  tthread=24
  scheduler="OG_BFS_A"
  defaultScheduler="OG_BFS_A"
  CCOption=3 #TSTREAM
  complexity=8000
  NUM_ITEMS=491520
  deposit_ratio=100
  key_skewness=0


  isCyclic=0
  isDynamic=0
  workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
  schedulerPool="OG_BFS_A,OG_NS_A,OP_NS_A,OP_NS"
  rootFilePath="/home/myc/workspace/jjzhao/expDir/result/MulticoreScalability"
  shiftRate=1
  multicoreEvaluation=1
  maxThreads=24
  totalEvents=`expr $checkpointInterval \* $maxThreads \* 13 \* $shiftRate`
}

function runTStream() {
  echo "java -Xms300g -Xmx300g -jar -d64 /home/myc/workspace/jjzhao/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --tthread $tthread \
          --scheduler $scheduler \
          --defaultScheduler $defaultScheduler \
          --checkpoint_interval $checkpointInterval \
          --CCOption $CCOption \
          --complexity $complexity \
          --deposit_ratio $deposit_ratio \
          --key_skewness $key_skewness \
          --isCyclic $isCyclic \
          --rootFilePath $rootFilePath \
          --isDynamic $isDynamic \
          --totalEvents $totalEvents \
          --shiftRate $shiftRate \
          --workloadType $workloadType \
          --schedulerPool $schedulerPool \
          --multicoreEvaluation $multicoreEvaluation \
          --maxThreads $maxThreads"
  java -Xms300g -Xmx300g -Xss100M -XX:+PrintGCDetails -Xmn150g -XX:+UseG1GC -jar -d64 /home/myc/workspace/jjzhao/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --defaultScheduler $defaultScheduler \
    --checkpoint_interval $checkpointInterval \
    --CCOption $CCOption \
    --complexity $complexity \
    --deposit_ratio $deposit_ratio \
    --key_skewness $key_skewness \
    --isCyclic $isCyclic \
    --rootFilePath $rootFilePath \
    --isDynamic $isDynamic \
    --totalEvents $totalEvents \
    --shiftRate $shiftRate \
    --workloadType $workloadType \
    --schedulerPool $schedulerPool \
    --multicoreEvaluation $multicoreEvaluation \
    --maxThreads $maxThreads
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  isDynamic=1
  runTStream
  ResetParameters

  scheduler=TStream
  isDynamic=0
  runTStream
  ResetParameters
}


function patEvluation() {
  isDynamic=0
  CCOption=4 #SSTORE
  runTStream
  ResetParameters
}


function runner() { # multi-batch exp
 ResetParameters
 for tthread in 1 4 8 12 16 20 24
   do
      app=StreamLedger
      baselineEvaluation
      patEvluation
   done
}
runner
ResetParameters