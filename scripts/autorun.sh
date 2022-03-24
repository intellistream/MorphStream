#!/bin/bash

FLINK_DIR="/home/myc/workspace/flink/build-target"
FLINK_APP_DIR="/home/myc/workspace/MorphStream"

# run flink clsuter
function runFlink() {
    echo "INFO: starting the cluster"
    if [[ -d ${FLINK_DIR}/log ]]; then
        rm -rf ${FLINK_DIR}/log
    fi
    mkdir ${FLINK_DIR}/log
    ${FLINK_DIR}/bin/start-cluster.sh
}

# clean app specific related data
function cleanEnv() {
    mv ${FLINK_DIR}/log/flink-myc-taskexecutor-0-myc-amd.out .
    rm -rf /tmp/flink*
    rm ${FLINK_DIR}/log/*
}

# clsoe flink clsuter
function stopFlink() {
    echo "INFO: experiment finished, stopping the cluster"
    PID=`jps | grep CliFrontend | awk '{print $1}'`
    if [[ ! -z $PID ]]; then
      kill -9 ${PID}
    fi
    ${FLINK_DIR}/bin/stop-cluster.sh
    echo "close finished"
    cleanEnv
}

# initialization of the parameters
init() {
  # app level
  JAR="${FLINK_APP_DIR}/target/StreamLedger-1.0-SNAPSHOT.jar"
  job="StreamLedger.Main"
}

# run applications
function runApp() {
  echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} &"
  ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} &
}


init
runFlink
runApp

python -c 'import time; time.sleep('"100"')'

stopFlink
