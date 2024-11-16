#!/bin/bash
source ../global.sh || exit
function ResetParameters() {
  app="StreamLedger"
  checkpointInterval=10240
  tthread=24
  scheduler="OG_BFS_A"
  defaultScheduler="OG_BFS_A"
  CCOption=3 #TSTREAM
  complexity=8000
  NUM_ITEMS=491520
  deposit_ratio=100
  key_skewness=0


  isCyclic=0
  isDynamic=0
  workloadType="default,unchanging,unchanging,unchanging,Up_skew,Up_skew,Up_skew,Up_PD,Up_PD,Up_PD,Up_abort,Up_abort,Up_abort"
  schedulerPool="OG_BFS_A,OG_NS_A,OP_NS_A,OP_NS"
  rootFilePath="${project_Dir}/result/data/VaryingJVMSize"
  shiftRate=1
  totalEvents=`expr $checkpointInterval \* $tthread \* 13 \* $shiftRate`
  cleanUp=1

  jvmConf="-Xms300g -Xmx300g -Xss100M -Xmn150g"
}

function runTStream() {
  echo "java $jvmConf ${jar_Dir} \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --tthread $tthread \
          --scheduler $scheduler \
          --defaultScheduler $defaultScheduler \
          --checkpoint_interval $checkpointInterval \
          --CCOption $CCOption \
          --complexity $complexity \
          --deposit_ratio $deposit_ratio \
          --key_skewness $key_skewness \
          --isCyclic $isCyclic \
          --rootFilePath $rootFilePath \
          --isDynamic $isDynamic \
          --totalEvents $totalEvents \
          --shiftRate $shiftRate \
          --workloadType $workloadType \
          --schedulerPool $schedulerPool \
          --cleanUp $cleanUp"
  java $jvmConf -XX:+PrintGCDetails  -XX:+UseG1GC -jar -d64 $jar_Dir \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --defaultScheduler $defaultScheduler \
    --checkpoint_interval $checkpointInterval \
    --CCOption $CCOption \
    --complexity $complexity \
    --deposit_ratio $deposit_ratio \
    --key_skewness $key_skewness \
    --isCyclic $isCyclic \
    --rootFilePath $rootFilePath \
    --isDynamic $isDynamic \
    --totalEvents $totalEvents \
    --shiftRate $shiftRate \
    --workloadType $workloadType \
    --schedulerPool $schedulerPool \
    --cleanUp $cleanUp
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  isDynamic=1
  runTStream
  ResetParameters
}


function patEvluation() {
  isDynamic=0
  CCOption=4 #SSTORE
  runTStream
}


function varying_JVMSize() { # multi-batch exp
 ResetParameters
 for jvmConf in "-Xms300g -Xmx300g -Xss100M -Xmn150g" "-Xms200g -Xmx200g -Xss100M -Xmn100g" "-Xms100g -Xmx100g -Xss100M -Xmn50g"
   do
    app=StreamLedger
    baselineEvaluation
    # 结果源目录
    sourceDir="result.example/data/VaryingJVMSize/stats/StreamLedger/OG_BFS_A/threads = 24/totalEvents = 3194880"

    # 生成以 JVM 配置命名的目标目录
    safeConf=$(echo "$jvmConf" | sed 's/ /_/g' | sed 's/[^a-zA-Z0-9_]/_/g')
    targetDir="result.example/data/VaryingJVMSize/stats/StreamLedger/OG_BFS_A/${safeConf}"

    # 创建目标目录
    mkdir -p "$targetDir"

    # 移动结果文件到目标目录
    mv "$sourceDir"/* "$targetDir"

    # 确保源目录清空后仍存在
    touch "$sourceDir/.placeholder"
   done
}
function withoutGC() {
  ResetParameters
  jvmConf="-Xms300g -Xmx300g -Xss100M"
  cleanUp=0
  app=StreamLedger
  baselineEvaluation
  ResetParameters
}
varying_JVMSize
withoutGC
ResetParameters