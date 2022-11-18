#!/bin/bash

PROJECT_DIR="/home/myc/workspace/myc/MorphStream"

FLINK_DIR=${PROJECT_DIR}/flink-1.10
REDIS_DIR=${PROJECT_DIR}/redis
FLINK_APP_DIR=${PROJECT_DIR}

# run flink clsuter
function runFlink() {
    echo "INFO: starting the Flink cluster"
    if [[ -d ${FLINK_DIR}/log ]]; then
        rm -rf ${FLINK_DIR}/log
    fi
    mkdir ${FLINK_DIR}/log
    ${FLINK_DIR}/bin/start-cluster.sh
}

function runRedis() {
  echo "INFO: starting Redis Server"
}

# clean app specific related data
function cleanEnv() {
    mv ${FLINK_DIR}/log/*.out ${job}.out
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

# clsoe flink clsuter
function stopRedis() {
    echo "INFO: stopping Redis Server"
}
    

# initialization of the parameters
initSL() {
  # app level
  JAR="${FLINK_APP_DIR}/target/StreamLedger-1.0-SNAPSHOT.jar"
  job="StreamLedger"
}

initSLNoLock() {
  # app level
  JAR="${FLINK_APP_DIR}/target/StreamLedger-1.0-SNAPSHOT.jar"
  job="StreamLedgerNoLock"
}

# run applications
function runApp() {
  echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} &"
  ${FLINK_DIR}/bin/flink run -c StreamLedger.${job} ${JAR} &
}

build () {
  mvn clean package
}


# Run SL Exps
initSL
runFlink
runRedis

runApp

python3 -c 'import time; time.sleep('"200"')'

stopRedis
stopFlink

# Run SLNoLock Exps
initSL
runFlink

runApp

python3 -c 'import time; time.sleep('"200"')'

stopRedis
stopFlink
