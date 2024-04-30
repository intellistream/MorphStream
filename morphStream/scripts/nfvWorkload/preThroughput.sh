#!/bin/bash

function ResetParameters() {
  app="nfv_test"
  checkpointInterval=100
  tthread=4
  scheduler="OG_BFS_A"
  defaultScheduler="OG_BFS_A"
  complexity=10000
  NUM_ITEMS=10000
  rootFilePath="/home/shuhao/jjzhao/data"
  totalEvents=40000

  serveRemoteVNF=false
  vnfInstanceNum=4
  offloadCCThreadNum=4
  rRatioSharedReaders=80
  wRatioSharedWriters=80
  rwRatioMutualInteractive=80
  ccStrategy=0
  workloadPattern=0
}


#@Parameter(names = {"--serveRemoteVNF"}, description = "True if vnf instances are connecting through socket, false if vnf instances are simulated locally")
 #    public boolean serveRemoteVNF = false;
 #    @Parameter(names = {"--vnfInstanceNum"}, description = "Number of socket listener to handle VNF instances, each for one VNF socket")
 #    public int vnfInstanceNum = 4;
 #    @Parameter(names = {"--offloadCCThreadNum"}, description = "Number of threads in Offloading CC's executor service thread pool")
 #    public int offloadCCThreadNum = 4;
 #    @Parameter(names = {"--rRatioSharedReaders"}, description = "Read ratio for shared readers pattern")
 #    public int rRatioSharedReaders = 80;
 #    @Parameter(names = {"--wRatioSharedWriters"}, description = "Write ratio for shared writers pattern")
 #    public int wRatioSharedWriters = 80;
 #    @Parameter(names = {"--rwRatioMutualInteractive"}, description = "Read-write ratio for mutual interactive pattern")
 #    public int rwRatioMutualInteractive = 80;
 #    @Parameter(names = {"--ccStrategy"}, description = "Chosen CC strategy")
 #    public int ccStrategy = 0;
 #    @Parameter(names = {"--workloadPattern"}, description = "Chosen pattern workload")
 #    public int workloadPattern = 0;

function runTStream() {
  echo "java -Xms300g -Xmx300g -jar -d64 /home/shuhao/jjzhao/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --tthread $tthread \
          --scheduler $scheduler \
          --defaultScheduler $defaultScheduler \
          --checkpoint_interval $checkpointInterval \
          --complexity $complexity \
          --rootFilePath $rootFilePath \
          --totalEvents $totalEvents \
          --serveRemoteVNF $serveRemoteVNF \
          --vnfInstanceNum $vnfInstanceNum \
          --offloadCCThreadNum $offloadCCThreadNum \
          --rRatioSharedReaders $rRatioSharedReaders \
          --wRatioSharedWriters $wRatioSharedWriters \
          --rwRatioMutualInteractive $rwRatioMutualInteractive \
          --ccStrategy $ccStrategy \
          --workloadPattern $workloadPattern
          "
  java -Xms300g -Xmx300g -Xss100M -jar -d64 /home/shuhao/jjzhao/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --defaultScheduler $defaultScheduler \
    --checkpoint_interval $checkpointInterval \
    --complexity $complexity \
    --rootFilePath $rootFilePath \
    --totalEvents $totalEvents \
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