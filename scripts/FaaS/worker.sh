#!/bin/bash
source ../../dir.sh || exit
Id=$1
DAGName=$2
number=$3
batch=$4
frontend=$5
worker=$6
thread=$7
s=$8
c=$9
function ResetParameters() {
    #Cluster Configurations
    isDriver=0
    isDatabase=0
    workerId=$Id
    workerNum=$worker
    tthread=$thread
    clientNum=20
    frontendNum=$frontend
    clientClassName="client.$DAGName"
    #Network Configurations
    isRDMA=1
    driverHost="10.10.10.19"
    driverPort=5590
    databaseHost="10.10.10.19"
    databasePort=5580
    workerHosts="10.10.10.20,10.10.10.24,10.10.10.3,10.10.10.113"
    workerPorts="5550,5540,5530,5520"
    CircularBufferCapacity=`expr 1024 \* 1024 \* 1024`
    TableBufferCapacity=`expr 1024 \* 1024 \* 1024`
    CacheBufferCapacity=`expr 1024 \* 1024 \* 1024`
    RemoteOperationBufferCapacity=`expr 1024 \* 1024 \* 1024`
    sendMessagePerFrontend=`expr $number \* $tthread \* $workerNum / $frontendNum`
    totalBatch=$batch
    returnResultPerExecutor=`expr $number`
    shuffleType=3
    #Database Configurations
    isRemoteDB=1
    numberItemsForTables="10000;10000;20000"
    NUM_ITEMS=20000
    tableNames="user_pwd;user_profile;tweet"
    keyDataTypesForTables="String;String;String"
    valueDataTypesForTables="String;String;String"
    valueDataSizesForTables="16;64;64"
    valueNamesForTables="password;profile;tweet"
    #Input Configurations
    rootFilePath="${RSTDIR}"
    inputFileType=0
    eventTypes="userLogin;userProfile;getTimeLine;postTweet"
    tableNameForEvents="user_pwd;user_profile;tweet;tweet"
    keyNumberForEvents="1;1;2;2"
    valueNameForEvents="password;;;tweet1,tweet2"
    valueSizeForEvents="16;0;0;64,64"
    eventRatio="0;0;90;10"
    ratioOfMultiPartitionTransactionsForEvents="0;0;0;0"
    stateAccessSkewnessForEvents="0;0;0;0"
    abortRatioForEvents="0;0;0;0"
    isCyclic=0
    isDynamic=0
    workloadType="default,unchanging,unchanging,unchanging"
    shiftRate=1
    checkpointInterval=`expr $sendMessagePerFrontend \* $frontendNum \* $totalBatch`
    totalEvents=`expr $checkpointInterval \* $shiftRate \* 1`
    #System Configurations
    schedulerPool="${s}Scheduler"
    scheduler="${s}Scheduler"
    defaultScheduler="${s}Scheduler"
    CCOption=$c #TSTREAM
    complexity=0
}

function runApplication() {
  echo "-Xms64g -Xmx64g -Xss100M -XX:+PrintGCDetails -Xmn60g -XX:+UseG1GC -jar -d64 ${JAR} -Djava.library.path=${LIBDIR} \
      --isDriver $isDriver \
      --isDatabase $isDatabase \
      --workerId $workerId \
      --workerNum $workerNum \
      --tthread $tthread \
      --clientNum $clientNum \
      --frontendNum $frontendNum \
      --clientClassName $clientClassName \
      --isRDMA $isRDMA \
      --driverHost $driverHost \
      --driverPort $driverPort \
      --databaseHost $databaseHost \
      --databasePort $databasePort \
      --workerHosts $workerHosts \
      --workerPorts $workerPorts \
      --CircularBufferCapacity $CircularBufferCapacity \
      --TableBufferCapacity $TableBufferCapacity \
      --CacheBufferCapacity $CacheBufferCapacity \
      --RemoteOperationBufferCapacity $RemoteOperationBufferCapacity \
      --sendMessagePerFrontend $sendMessagePerFrontend \
      --returnResultPerExecutor $returnResultPerExecutor \
      --totalBatch $totalBatch \
      --shuffleType $shuffleType \
      --isRemoteDB $isRemoteDB \
      --numberItemsForTables $numberItemsForTables \
      --NUM_ITEMS $NUM_ITEMS \
      --tableNames $tableNames \
      --keyDataTypesForTables $keyDataTypesForTables \
      --valueDataTypesForTables $valueDataTypesForTables \
      --valueDataSizeForTables $valueDataSizesForTables \
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
      --valueSizeForEvents $valueSizeForEvents \
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
  java -Xms48g -Xmx48g -Xss100M -XX:+PrintGCDetails -Xmn40g -XX:+UseG1GC -Djava.library.path=$LIBDIR -jar $JAR \
      --isDriver $isDriver \
      --isDatabase $isDatabase \
      --workerId $workerId \
      --workerNum $workerNum \
      --tthread $tthread \
      --clientNum $clientNum \
      --frontendNum $frontendNum \
      --clientClassName $clientClassName \
      --isRDMA $isRDMA \
      --driverHost $driverHost \
      --driverPort $driverPort \
      --databaseHost $databaseHost \
      --databasePort $databasePort \
      --workerHosts $workerHosts \
      --workerPorts $workerPorts \
      --CircularBufferCapacity $CircularBufferCapacity \
      --TableBufferCapacity $TableBufferCapacity \
      --CacheBufferCapacity $CacheBufferCapacity \
      --RemoteOperationBufferCapacity $RemoteOperationBufferCapacity \
      --sendMessagePerFrontend $sendMessagePerFrontend \
      --returnResultPerExecutor $returnResultPerExecutor \
      --totalBatch $totalBatch \
      --shuffleType $shuffleType \
      --isRemoteDB $isRemoteDB \
      --numberItemsForTables $numberItemsForTables \
      --NUM_ITEMS $NUM_ITEMS \
      --tableNames $tableNames \
      --keyDataTypesForTables $keyDataTypesForTables \
      --valueDataTypesForTables $valueDataTypesForTables \
      --valueDataSizeForTables $valueDataSizesForTables \
      --valueNamesForTables $valueNamesForTables \
      --rootFilePath $rootFilePath \
      --inputFileType $inputFileType \
      --eventTypes $eventTypes \
      --tableNameForEvents $tableNameForEvents \
      --keyNumberForEvents $keyNumberForEvents \
      --valueNameForEvents $valueNameForEvents \
      --valueSizeForEvents $valueSizeForEvents \
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

