#!/bin/bash
function ResetParameters() {
    #Cluster Configurations
    isDriver=0
    isDatabase=1
    isClient=0
    workerId=0
    workerNum=$workerNum
    tthread=$threadNum
    clientNum=$clientNum
    frontendNum=$frontendNum
    clientClassName="client.$DAGName"
    #Network Configurations
    isRDMA=1
    driverHost=$driverHost
    driverPort=$driverPort
    databaseHost=$databaseHost
    databasePort=$databasePort
    workerHosts=$workerHosts
    workerPorts=$workerPorts
    CircularBufferCapacity=`expr 1024 \* 1024 \* 1024`
    TableBufferCapacity=`expr 1024 \* 1024 \* 1024`
    CacheBufferCapacity=`expr 1024 \* 1024 \* 1024`
    RemoteOperationBufferCapacity=`expr 1024 \* 1024 \* 1024`
    sendMessagePerFrontend=`expr $number \* $threadNum \* $workerNum / $frontendNum`
    totalBatch=$batch
    returnResultPerExecutor=`expr $number`
    shuffleType=$shuffleType
    #Database Configurations
    isRemoteDB=$isRemoteDB
    isDynamoDB=$isDynamoDB
    r_w_capacity_unit=$r_w_capacity_unit
    numberItemsForTables=$numberItemsForTables
    NUM_ITEMS=$NUM_ITEMS
    tableNames=$tableNames
    keyDataTypesForTables=$keyDataTypesForTables
    valueDataTypesForTables=$valueDataTypesForTables
    valueDataSizeForTables=$valueDataSizeForTables
    valueNamesForTables=$valueNamesForTables
    #Input Configurations
    rootFilePath="${RSTDIR}"
    inputFileType=0
    eventTypes=$eventTypes
    tableNameForEvents=$tableNameForEvents
    keyNumberForEvents=$keyNumberForEvents
    valueNameForEvents=$valueNameForEvents
    valueSizeForEvents=$valueSizeForEvents
    eventRatio=$eventRatio
    ratioOfMultiPartitionTransactionsForEvents=$ratioOfMultiPartitionTransactionsForEvents
    stateAccessSkewnessForEvents=$stateAccessSkewnessForEvents
    abortRatioForEvents=$abortRatioForEvents
    isCyclic=0
    isDynamic=0
    workloadType="default,unchanging,unchanging,unchanging"
    shiftRate=1
    checkpointInterval=`expr $sendMessagePerFrontend \* $frontendNum \* $batch`
    totalEvents=`expr $checkpointInterval \* $shiftRate \* 1`
    #System Configurations
    schedulerPool=$schedulerPool
    scheduler=$scheduler
    defaultScheduler=$defaultScheduler
    CCOption=3 #TSTREAM
    complexity=0
}

function runApplication() {
  echo "-Xms60g -Xmx60g -Xss100M -XX:+PrintGCDetails -Xmn40g -XX:+UseG1GC -jar -d64 ${JAR} -Djava.library.path=${LIBDIR} \
      --isDriver $isDriver \
      --isDatabase $isDatabase \
      --isClient $isClient \
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
      --isDynamoDB $isDynamoDB \
      --r_w_capacity_unit $r_w_capacity_unit \
      --numberItemsForTables $numberItemsForTables \
      --NUM_ITEMS $NUM_ITEMS \
      --tableNames $tableNames \
      --keyDataTypesForTables $keyDataTypesForTables \
      --valueDataTypesForTables $valueDataTypesForTables \
      --valueNamesForTables $valueNamesForTables \
      --valueDataSizeForTables $valueDataSizeForTables \
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
      --complexity $complexity \
            "
  java -Xms100g -Xmx100g -Xss100M -XX:+PrintGCDetails -Xmn80g -XX:+UseG1GC -Djava.library.path=$LIBDIR -jar -d64 $JAR \
      --isDriver $isDriver \
      --isClient $isClient \
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
      --isDynamoDB $isDynamoDB \
      --r_w_capacity_unit $r_w_capacity_unit \
      --numberItemsForTables $numberItemsForTables \
      --NUM_ITEMS $NUM_ITEMS \
      --tableNames $tableNames \
      --keyDataTypesForTables $keyDataTypesForTables \
      --valueDataTypesForTables $valueDataTypesForTables \
      --valueNamesForTables $valueNamesForTables \
      --valueDataSizeForTables $valueDataSizeForTables \
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
rm -rf ${RSTDIR}/inputs/client.$DAGName