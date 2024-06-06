#!/bin/bash

function ResetParameters() {
  app="nfv_test"
  checkpointInterval=500
  tthread=8
  scheduler="OP_BFS_A"
  defaultScheduler="OP_BFS_A"
  complexity=0
  NUM_ITEMS=10000
  rootFilePath="/home/shuhao/jjzhao/data"
  totalEvents=400000

  nfvWorkloadPath="/home/shuhao/DB4NFV/morphStream/scripts/TransNFV"
  communicationChoice=0
  vnfInstanceNum=4
  offloadCCThreadNum=16
  offloadLockNum=10000
  rRatioSharedReaders=80
  wRatioSharedWriters=80
  rwRatioMutualInteractive=80
  ccStrategy=0
  workloadPattern=0
  enableTimeBreakdown=0
  experimentID="5.1"
}

function runTStream() {
  echo "java -Xms100g -Xmx100g -jar -d64 /home/shuhao/DB4NFV/morphStream/morph-clients/target/morph-clients-0.1.jar \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --tthread $tthread \
          --scheduler $scheduler \
          --defaultScheduler $defaultScheduler \
          --checkpoint_interval $checkpointInterval \
          --complexity $complexity \
          --rootFilePath $rootFilePath \
          --totalEvents $totalEvents \
          --nfvWorkloadPath $nfvWorkloadPath \
          --communicationChoice $communicationChoice \
          --vnfInstanceNum $vnfInstanceNum \
          --offloadCCThreadNum $offloadCCThreadNum \
          --offloadLockNum $offloadLockNum \
          --rRatioSharedReaders $rRatioSharedReaders \
          --wRatioSharedWriters $wRatioSharedWriters \
          --rwRatioMutualInteractive $rwRatioMutualInteractive \
          --ccStrategy $ccStrategy \
          --workloadPattern $workloadPattern \
          --enableTimeBreakdown $enableTimeBreakdown \
          --experimentID $experimentID
          "
  java -Xms20g -Xmx80g -Xss10M -jar -d64 /home/shuhao/DB4NFV/morphStream/morph-clients/target/morph-clients-0.1.jar \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --defaultScheduler $defaultScheduler \
    --checkpoint_interval $checkpointInterval \
    --complexity $complexity \
    --rootFilePath $rootFilePath \
    --totalEvents $totalEvents \
    --nfvWorkloadPath $nfvWorkloadPath \
    --communicationChoice $communicationChoice \
    --vnfInstanceNum $vnfInstanceNum \
    --offloadCCThreadNum $offloadCCThreadNum \
    --offloadLockNum $offloadLockNum \
    --rRatioSharedReaders $rRatioSharedReaders \
    --wRatioSharedWriters $wRatioSharedWriters \
    --rwRatioMutualInteractive $rwRatioMutualInteractive \
    --ccStrategy $ccStrategy \
    --workloadPattern $workloadPattern \
    --enableTimeBreakdown $enableTimeBreakdown \
    --experimentID $experimentID
}

function baselinePattern() {
  ResetParameters
  for workloadPattern in 0 1 2 3
  do
    for ccStrategy in 0 1 2 3
    do
      runTStream
    done
  done
}

baselinePattern
ResetParameters
