#!/bin/bash
source dir.sh || exit
function ResetParameters() {
    app="GrepSum"
    checkpointInterval=40960
    tthread=24
    scheduler="OG_NS_A"
    defaultScheduler="OG_NS_A"
    CCOption=3 #TSTREAM
    complexity=8000
    NUM_ITEMS=245760
    abort_ratio=0
    multiple_ratio=0
    txn_length=1
    NUM_ACCESS=1
    key_skewness=75
    overlap_ratio=0
    isCyclic=0
    isDynamic=1
    workloadType="default,unchanging,unchanging,unchanging"
  # workloadType="default,unchanging,unchanging,unchanging,Up_abort,Down_abort,unchanging,unchanging"
  # workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
    schedulerPool="OG_NS_A"
    rootFilePath="${RSTDIR}"
    shiftRate=1
    multicoreEvaluation=0
    maxThreads=20
    totalEvents=`expr $checkpointInterval \* $tthread \* 4 \* $shiftRate`

    snapshotInterval=4
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
              --abort_ratio $abort_ratio \
              --multiple_ratio $multiple_ratio \
              --overlap_ratio $overlap_ratio \
              --txn_length $txn_length \
              --NUM_ACCESS $NUM_ACCESS \
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
    java -Xms300g -Xmx300g -Xss100M -XX:+PrintGCDetails -Xmn200g -XX:+UseG1GC -jar -d64 $JAR \
      --app $app \
      --NUM_ITEMS $NUM_ITEMS \
      --tthread $tthread \
      --scheduler $scheduler \
      --defaultScheduler $defaultScheduler \
      --checkpoint_interval $checkpointInterval \
      --CCOption $CCOption \
      --complexity $complexity \
      --abort_ratio $abort_ratio \
      --multiple_ratio $multiple_ratio \
      --overlap_ratio $overlap_ratio \
      --txn_length $txn_length \
      --NUM_ACCESS $NUM_ACCESS \
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
 ResetParameters
 app=GrepSum
 for FTOption in 4 5 6
 do
 #withoutRecovery
 withRecovery
 done
}
application_runner
