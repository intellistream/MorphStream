#!/bin/bash
source dir.sh || exit
function ResetParameters() {
    app="TollProcessing"
    checkpointInterval=40960
    tthread=24
    scheduler="OG_NS_A"
    defaultScheduler="OG_NS_A"
    CCOption=3 #TSTREAM
    complexity=8000
    NUM_ITEMS=491520
    abort_ratio=3000
    overlap_ratio=10
    key_skewness=30
    isDynamic=1
    workloadType="default,unchanging,unchanging,unchanging"
  # workloadType="default,unchanging,unchanging,unchanging,Up_abort,Down_abort,unchanging,unchanging"
  # workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
    schedulerPool="OG_NS_A,OG_NS"
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
    failureTime=25000
    measureInterval=100
    compressionAlg="None"
    isSelective=0
    maxItr=0

    isHistoryView=1
    isAbortPushDown=1
    isTaskPlacing=1
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
              --overlap_ratio $overlap_ratio \
              --key_skewness $key_skewness \
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
              --maxItr $maxItr \
              --isHistoryView $isHistoryView \
              --isAbortPushDown $isAbortPushDown \
              --isTaskPlacing $isTaskPlacing"
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
      --overlap_ratio $overlap_ratio \
      --key_skewness $key_skewness \
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
      --maxItr $maxItr \
      --isHistoryView $isHistoryView \
      --isAbortPushDown $isAbortPushDown \
      --isTaskPlacing $isTaskPlacing
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
#  app=TollProcessing
#  for FTOption in 1
#  do
#  #withoutRecovery
#  withRecovery
#  done

#  for FTOption in 3
#  do
#  isHistoryView=1
#  isAbortPushDown=0
#  isTaskPlacing=0
#  #withoutRecovery
#  withRecovery
#  done

#  for FTOption in 3
#  do
#  isHistoryView=1
#  isAbortPushDown=1
#  isTaskPlacing=0
#  #withoutRecovery
#  withRecovery
#  done

 for FTOption in 3
 do
 isHistoryView=1
 isAbortPushDown=1
 isTaskPlacing=1
 #withoutRecovery
 withRecovery
 done
}
application_runner
