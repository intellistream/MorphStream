#!/bin/bash
source ../dir.sh || exit
function ResetParameters() {
    #Cluster Configurations
    isDriver=1
    workerId=0
    workerNum=1
    tthread=4
    clientNum=4
    frontendNum=4
    clientClassName="client.BankingSystemClient"
    #Network Configurations
    isRDMA=1
    driverHost="10.10.10.1"
    driverPort=5570
    workerHosts="10.10.10.2"
    workerPorts="5540"
    CircularBufferCapacity=`expr 1024 \* 1024 \* 1024`
    TableBufferCapacity=`expr 1024 \* 1024 \* 1024`
    sendMessagePerFrontend=`expr 2000 \* $tthread \* $workerNum / $frontendNum`
    totalBatch=5
    returnResultPerExecutor=`expr 10000 \* $frontendNum / $workerNum / $tthread`
    shuffleType=3
    #Database Configurations
    numberItemsForTables="8000,8000"
    NUM_ITEMS=8000
    tableNames="accounts,bookEntries"
    keyDataTypesForTables="String,String"
    valueDataTypesForTables="double,double"
    valueNamesForTables="balance,balance"
    #Input Configurations
    rootFilePath="${RSTDIR}"
    inputFileType=0
    eventTypes="transfer;deposit"
    tableNameForEvents="accounts,bookEntries;accounts,bookEntries"
    keyNumberForEvents="2,2;1,1"
    valueNameForEvents="transferAmount,transferAmount;depositAmount,depositAmount"
    eventRatio="50,50"
    ratioOfMultiPartitionTransactionsForEvents="0,0"
    stateAccessSkewnessForEvents="0,0"
    abortRatioForEvents="0,0"
    isCyclic=0
    isDynamic=1
    workloadType="default,unchanging,unchanging,unchanging"
    shiftRate=1
    checkpointInterval=`expr $sendMessagePerFrontend \* $frontendNum \* $totalBatch`
    totalEvents=`expr $checkpointInterval \* $shiftRate \* 1`
    #System Configurations
    schedulerPool="OP_BFS_A"
    scheduler="OP_BFS_A"
    defaultScheduler="OP_BFS_A"
    CCOption=3 #TSTREAM
    complexity=0
}

function runApplication() {
  echo "-Xms60g -Xmx60g -Xss100M -XX:+PrintGCDetails -Xmn40g -XX:+UseG1GC -jar -d64 ${JAR} -Djava.library.path=/home/jjzhao/local/lib \
      --isDriver $isDriver \
      --workerId $workerId \
      --workerNum $workerNum \
      --tthread $tthread \
      --clientNum $clientNum \
      --frontendNum $frontendNum \
      --clientClassName $clientClassName \
      --isRDMA $isRDMA \
      --driverHost $driverHost \
      --driverPort $driverPort \
      --workerHosts $workerHosts \
      --workerPorts $workerPorts \
      --CircularBufferCapacity $CircularBufferCapacity \
      --TableBufferCapacity $TableBufferCapacity \
      --sendMessagePerFrontend $sendMessagePerFrontend \
      --returnResultPerExecutor $returnResultPerExecutor \
      --totalBatch $totalBatch \
      --shuffleType $shuffleType \
      --numberItemsForTables $numberItemsForTables \
      --NUM_ITEMS $NUM_ITEMS \
      --tableNames $tableNames \
      --keyDataTypesForTables $keyDataTypesForTables \
      --valueDataTypesForTables $valueDataTypesForTables \
      --valueNamesForTables $valueNamesForTables \
      --rootFilePath $rootFilePath \
      --inputFileType $inputFileType \
      --eventTypes $eventTypes \
      --tableNameForEvents $tableNameForEvents \
      --keyNumberForEvents $keyNumberForEvents \
      --valueNameForEvents $valueNameForEvents \
      --eventRatio $eventRatio \
      --ratioOfMultiPartitionTransactionsForEvents $ratioOfMultiPartitionTransactionsForEvents \
      --stateAccessSkewnessForEvents $stateAccessSkewnessForEvents \
      --abortRatioForEvents $abortRatioForEvents \
      --isCyclic $isCyclic \
      --isDynamic $isDynamic \
      --workloadType $workloadType \
      --shiftRate $shiftRate \
      --totalEvents $totalEvents \
      --schedulerPool $schedulerPool \
      --checkpoint_interval $checkpointInterval \
      --scheduler $scheduler \
      --defaultScheduler $defaultScheduler \
      --CCOption $CCOption \
      --complexity $complexity \
            "
  java -Xms60g -Xmx60g -Xss100M -XX:+PrintGCDetails -Xmn40g -XX:+UseG1GC -Djava.library.path=/home/jjzhao/local/disni/lib -jar -d64 $JAR \
      --isDriver $isDriver \
      --workerId $workerId \
      --workerNum $workerNum \
      --tthread $tthread \
      --clientNum $clientNum \
      --frontendNum $frontendNum \
      --clientClassName $clientClassName \
      --isRDMA $isRDMA \
      --driverHost $driverHost \
      --driverPort $driverPort \
      --workerHosts $workerHosts \
      --workerPorts $workerPorts \
      --CircularBufferCapacity $CircularBufferCapacity \
      --TableBufferCapacity $TableBufferCapacity \
      --sendMessagePerFrontend $sendMessagePerFrontend \
      --returnResultPerExecutor $returnResultPerExecutor \
      --totalBatch $totalBatch \
      --shuffleType $shuffleType \
      --numberItemsForTables $numberItemsForTables \
      --NUM_ITEMS $NUM_ITEMS \
      --tableNames $tableNames \
      --keyDataTypesForTables $keyDataTypesForTables \
      --valueDataTypesForTables $valueDataTypesForTables \
      --valueNamesForTables $valueNamesForTables \
      --rootFilePath $rootFilePath \
      --inputFileType $inputFileType \
      --eventTypes $eventTypes \
      --tableNameForEvents $tableNameForEvents \
      --keyNumberForEvents $keyNumberForEvents \
      --valueNameForEvents $valueNameForEvents \
      --eventRatio $eventRatio \
      --ratioOfMultiPartitionTransactionsForEvents $ratioOfMultiPartitionTransactionsForEvents \
      --stateAccessSkewnessForEvents $stateAccessSkewnessForEvents \
      --abortRatioForEvents $abortRatioForEvents \
      --isCyclic $isCyclic \
      --isDynamic $isDynamic \
      --workloadType $workloadType \
      --shiftRate $shiftRate \
      --totalEvents $totalEvents \
      --schedulerPool $schedulerPool \
      --checkpoint_interval $checkpointInterval \
      --scheduler $scheduler \
      --defaultScheduler $defaultScheduler \
      --CCOption $CCOption \
      --complexity $complexity
}

function application_runner() {
 ResetParameters
 runApplication
}
application_runner
