#!/bin/bash

function ResetParameters() {
  app="StreamLedger"
  checkpointInterval=10240
  tthread=24
  scheduler="OG_BFS_A"
  defaultScheduler="OG_BFS_A"
  CCOption=3 #TSTREAM
  complexity=10000
  NUM_ITEMS=491520
  deposit_ratio=95
  key_skewness=20


  isCyclic=0
  isDynamic=0
  workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
  schedulerPool="OG_BFS_A,OG_NS_A,OP_NS_A,OP_NS"
  rootFilePath="/home/shuhao/jjzhao/data"
  shiftRate=1
  totalEvents=`expr $checkpointInterval \* $tthread \* 4 \* $shiftRate`
}

function runTStream() {
  echo "java -Xms300g -Xmx300g -jar -d64 /home/shuhao/jjzhao/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
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
          --schedulerPool $schedulerPool"
  java -Xms300g -Xmx300g -Xss100M -jar -d64 /home/shuhao/jjzhao/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
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
    --schedulerPool $schedulerPool
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  isDynamic=1
  runTStream
  ResetParameters

  scheduler=TStream
  workloadType="default"
  isDynamic=0
  runTStream
}


function patEvluation() {
  isDynamic=0
  CCOption=4 #SSTORE
  workloadType="default"
  runTStream
}


function dynamic_runner_everyPhase() { # multi-batch exp
 ResetParameters
 for workloadType in "default,unchanging,unchanging,unchanging" "default,Up_skew,Up_skew,Up_skew"
 do
     baselineEvaluation
     patEvluation
 done
 ResetParameters
 key_skewness=80
 workloadType="default,Up_PD,Up_PD,Up_PD"
 baselineEvaluation
 patEvluation
 ResetParameters
 deposit_ratio=35
 workloadType="default,Up_abort,Up_abort,Up_abort"
 baselineEvaluation
 patEvluation
}
dynamic_runner
ResetParameters