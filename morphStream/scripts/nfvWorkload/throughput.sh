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
  totalEvents=400000

  nfvWorkloadPath="/home/shuhao/DB4NFV/morphStream/scripts/nfvWorkload"
  serveRemoteVNF=1
  vnfInstanceNum=4
  offloadCCThreadNum=4
  rRatioSharedReaders=80
  wRatioSharedWriters=80
  rwRatioMutualInteractive=80
  ccStrategy=0
  workloadPattern=0
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
          --serveRemoteVNF $serveRemoteVNF \
          --vnfInstanceNum $vnfInstanceNum \
          --offloadCCThreadNum $offloadCCThreadNum \
          --rRatioSharedReaders $rRatioSharedReaders \
          --wRatioSharedWriters $wRatioSharedWriters \
          --rwRatioMutualInteractive $rwRatioMutualInteractive \
          --ccStrategy $ccStrategy \
          --workloadPattern $workloadPattern
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
    --serveRemoteVNF $serveRemoteVNF \
    --vnfInstanceNum $vnfInstanceNum \
    --offloadCCThreadNum $offloadCCThreadNum \
    --rRatioSharedReaders $rRatioSharedReaders \
    --wRatioSharedWriters $wRatioSharedWriters \
    --rwRatioMutualInteractive $rwRatioMutualInteractive \
    --ccStrategy $ccStrategy \
    --workloadPattern $workloadPattern
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