#!/bin/bash

function ResetParameters() {
  app="nfv_test"
  checkpointInterval=100
  tthread=4
  scheduler="OP_BFS_A"
  defaultScheduler="OP_BFS_A"
  complexity=0
  NUM_ITEMS=10000
  rootFilePath="/home/shuhao/jjzhao/data"
  totalEvents=4000

  nfvWorkloadPath="/home/shuhao/DB4NFV/morphStream/scripts/TransNFV"
  communicationChoice=0
  vnfInstanceNum=4
  offloadCCThreadNum=4
  offloadLockNum=1000
  rRatioSharedReaders=80
  wRatioSharedWriters=80
  rwRatioMutualInteractive=80
  ccStrategy=0
  workloadPattern=0
  enableCCSwitch=0
  experimentID="5.2.1_throughput"
}

function runTStream() {
  echo "java -Xms20g -Xmx80g -jar -d64 /home/shuhao/DB4NFV/morphStream/morph-clients/target/morph-clients-0.1.jar \
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
          --enableCCSwitch $enableCCSwitch \
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
    --enableCCSwitch $enableCCSwitch \
    --experimentID $experimentID
}

function baselinePattern() {
  ResetParameters
  for workloadPattern in 0 1 2 3
  do
    for ccStrategy in 0 1 2 3 4 5
    do
      runTStream
    done
  done
}

baselinePattern
ResetParameters
