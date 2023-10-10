#!/bin/bash
source dir.sh || exit
function ResetParameters() {
    app="StreamLedger"
    tthread=24
    scheduler="OP_BFS"
    defaultScheduler="OP_BFS"
    CCOption=3 #TSTREAM
    complexity=8000
    NUM_ITEMS=491520
    overlap_ratio=10
    abort_ratio=0
    deposit_ratio=0
    key_skewness=0
    isCyclic=1
    isDynamic=1
    workloadType="default"
  # workloadType="default,unchanging,unchanging,unchanging,Up_abort,Down_abort,unchanging,unchanging"
  # workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
    schedulerPool="OP_BFS"
    rootFilePath="${RSTDIR}"
    shiftRate=1
    multicoreEvaluation=0
    maxThreads=20

    arrivalControl=1
    arrivalRate=300
    FTOption=0
    isRecovery=0
    isFailure=0
    failureTime=250000
    measureInterval=100
    compressionAlg="None"
    isSelective=0
    maxItr=0
}

function runApplication() {
  echo "java -Xms300g -Xmx300g -Xss100M -XX:+PrintGCDetails -Xmn200g -XX:+UseG1GC -jar -d64 ${JAR} \
              --app $app \
              --NUM_ITEMS $NUM_ITEMS \
              --tthread $tthread \
              --scheduler $scheduler \
              --defaultScheduler $defaultScheduler \
              --checkpoint_interval $checkpointInterval \
              --CCOption $CCOption \
              --complexity $complexity \
              --deposit_ratio $deposit_ratio \
              --overlap_ratio $overlap_ratio \
              --abort_ratio $abort_ratio \
              --key_skewness $key_skewness \
              --isCyclic $isCyclic \
              --rootFilePath $rootFilePath \
              --isDynamic $isDynamic \
              --totalEvents $totalEvents \
              --shiftRate $shiftRate \
              --workloadType $workloadType \
              --schedulerPool $schedulerPool \
              --multicoreEvaluation $multicoreEvaluation \
              --maxThreads $maxThreads \
              --snapshotInterval $snapshotInterval \
              --arrivalControl $arrivalControl \
              --arrivalRate $arrivalRate \
              --FTOption $FTOption \
              --isRecovery $isRecovery \
              --isFailure $isFailure \
              --failureTime $failureTime \
              --measureInterval $measureInterval \
              --compressionAlg $compressionAlg \
              --isSelective $isSelective \
              --maxItr $maxItr"
    java -Xms400g -Xmx400g -Xss100M -XX:+PrintGCDetails -Xmn300g -XX:+UseG1GC -jar -d64 $JAR \
      --app $app \
      --NUM_ITEMS $NUM_ITEMS \
      --tthread $tthread \
      --scheduler $scheduler \
      --defaultScheduler $defaultScheduler \
      --checkpoint_interval $checkpointInterval \
      --CCOption $CCOption \
      --complexity $complexity \
      --deposit_ratio $deposit_ratio \
      --overlap_ratio $overlap_ratio \
      --abort_ratio $abort_ratio \
      --key_skewness $key_skewness \
      --isCyclic $isCyclic \
      --rootFilePath $rootFilePath \
      --isDynamic $isDynamic \
      --totalEvents $totalEvents \
      --shiftRate $shiftRate \
      --workloadType $workloadType \
      --schedulerPool $schedulerPool \
      --multicoreEvaluation $multicoreEvaluation \
      --maxThreads $maxThreads \
      --snapshotInterval $snapshotInterval \
      --arrivalControl $arrivalControl \
      --arrivalRate $arrivalRate \
      --FTOption $FTOption \
      --isRecovery $isRecovery \
      --isFailure $isFailure \
      --failureTime $failureTime \
      --measureInterval $measureInterval \
      --compressionAlg $compressionAlg \
      --isSelective $isSelective \
      --maxItr $maxItr
}
function withRecovery() {
    isFailure=1
    isRecovery=0
    runApplication
    sleep 2s
    isFailure=0
    isRecovery=1
    runApplication
}
function withoutRecovery() {
  runApplication
  sleep 2s
}

function application_runner() {
 for FTOption in 3
 do
 withoutRecovery
 #withRecovery
 done
}
function varyingEpoch() {
    ResetParameters
    app=StreamLedger
    for phase in 1 2 4 8 16
    do
    checkpointInterval=`expr 163840 / $phase` 
    snapshotInterval=$phase
    totalEvents=`expr $checkpointInterval \* $tthread \* $phase \* $shiftRate`
    application_runner
    done
}
varyingEpoch
