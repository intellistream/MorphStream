#!/bin/bash
source ../global.sh || exit
function ResetParameters() {
  app="TollProcessing"
  checkpointInterval=40960
  tthread=24
  scheduler="OG_DFS_A"
  CCOption=3 #TSTREAM
  complexity=10000
  NUM_ITEMS=495120
  isCyclic=0
  isGroup=0
  groupNum=1
  SchedulersForGroup="OG_DFS_A,OG_NS";
  skewGroup="20,80"
  high_abort_ratio=1500
  rootFilePath="${project_Dir}/result/data/Multiple"
}

function runTStream() {
  totalEvents=`expr $checkpointInterval \* $tthread`
  echo "java -Xms100g -Xmx100g -jar -d64 ${jar_Dir} \
          --app $app \
          --NUM_ITEMS $NUM_ITEMS \
          --tthread $tthread \
          --scheduler $scheduler \
          --checkpoint_interval $checkpointInterval \
          --CCOption $CCOption \
          --complexity $complexity \
          --isCyclic $isCyclic \
          --rootFilePath $rootFilePath \
          --isGroup $isGroup \
          --totalEvents $totalEvents \
          --groupNum $groupNum \
          --skewGroup $skewGroup \
          --SchedulersForGroup $SchedulersForGroup \
          --high_abort_ratio $high_abort_ratio"
  java -Xms100g -Xmx100g -Xss100M -jar -d64 $jar_Dir \
    --app $app \
    --NUM_ITEMS $NUM_ITEMS \
    --tthread $tthread \
    --scheduler $scheduler \
    --checkpoint_interval $checkpointInterval \
    --CCOption $CCOption \
    --complexity $complexity \
    --isCyclic $isCyclic \
    --rootFilePath $rootFilePath \
    --isGroup $isGroup \
    --totalEvents $totalEvents \
    --groupNum $groupNum \
    --skewGroup $skewGroup \
    --SchedulersForGroup $SchedulersForGroup \
    --high_abort_ratio $high_abort_ratio
}

# run basic experiment for different algorithms
function baselineEvaluation() {
  isGroup=1
  groupNum=2
  runTStream
  move_files_and_delete_folder "OG_DFS_A" "Nested"

  ResetParameters
  for scheduler in TStream OG_DFS_A OG_NS
  do
      runTStream
  done
}


function patEvluation() {
  isGroup=0
  CCOption=4 #SSTORE
  runTStream
}


function group_runner() { # multi-batch exp
 ResetParameters
 app=TollProcessing
 baselineEvaluation
 ResetParameters
 patEvluation
}
#!/bin/bash

# 定义移动文件并删除原文件夹的函数
move_files_and_delete_folder() {
  # 参数: $1 - "OG_DFS_A" 或其他源文件夹名，$2 - "Nested" 或其他目标文件夹名
  SOURCE_PART="$1"
  TARGET_PART="$2"

  # 基础路径（公共部分）
  BASE_PATH="${project_Dir}/data/Multiple/stats/TollProcessing"

  # 源文件夹路径
  SOURCE_DIR="$BASE_PATH/$SOURCE_PART/threads = 24/totalEvents = 983040/"

  # 目标文件夹路径
  TARGET_DIR="$BASE_PATH/$TARGET_PART/threads = 24/totalEvents = 983040/"

  # 检查目标文件夹是否存在，如果不存在则创建它
  if [ ! -d "$TARGET_DIR" ]; then
    mkdir -p "$TARGET_DIR"
  fi

  # 移动所有文件到目标文件夹
  mv "$SOURCE_DIR"* "$TARGET_DIR"

  # 删除源文件夹及其内容
  rm -r "$SOURCE_DIR"
}


group_runner
ResetParameters