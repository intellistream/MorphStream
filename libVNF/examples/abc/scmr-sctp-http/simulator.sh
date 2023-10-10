#!/usr/bin/env bash

source simulation_parameters.sh

CURRENT_TIME=$(date +'%b%d-%H:%M:%S%Z')
TEST_ID="${TEST_NAME}@${CURRENT_TIME}"
TEST_DIR="~/libvnf-tests/\"${TEST_ID}\""

deploy_and_start_c () {
    log "Deploying and starting ${C_TARGET}"

    local SCREEN_NAME="libvnf-${TEST_NAME}-${C_TARGET}"
    # Dir to contain C's files
    local DIR="${TEST_DIR}/${C_TARGET}"
    ssh -t ${C_VNF} "mkdir -p ${DIR}"
    # Copy C's source code there
    scp ${C_FILES} ${C_VNF}:${DIR}
    # Make C and run it
    ssh ${C_VNF} "
        cd ${DIR};
        make ${C_TARGET};
        # Kill existing screen
        screen -list | grep ${SCREEN_NAME} | cut -d. -f1 | awk '{print $1}' | xargs kill 2>/dev/null;
        screen -L -Logfile ${C_TARGET}.log -dmS ${SCREEN_NAME} ./${C_TARGET} ${C_IP} ${C_PORT}
        "

    # Check if C is running or not
    if [[ "$(ssh ${C_VNF} "screen -list | grep ${SCREEN_NAME}")" ]]; then
        return 0
    else
        log "Not able to start C"
        return 1
    fi
}

install_libvnf_in_b_vnf () {
    log "libvnf is being installed"

    # Dir to contain libvnf files
    local DIR="${TEST_DIR}/libvnf"
    ssh -t ${B_VNF} "mkdir -p ${DIR}"
    # Copy libvnf source code there
    scp -r ${LIBVNF_FILES} ${B_VNF}:${DIR}
    # Make libvnf and install it
    ssh -t ${B_VNF} "
        cd ${DIR}; mkdir build; cd build;
        ${LIBVNF_CMAKE_COMMAND};
        ${LIBVNF_MAKE_INSTALL_COMMAND};
        "
}

deploy_and_start_b () {
    log "Deploying and starting ${B_TARGET}"

    local SCREEN_NAME="libvnf-${TEST_NAME}-${B_TARGET}"
    # Dir to contain B's files
    local DIR="${TEST_DIR}/${B_TARGET}"
    ssh -t ${B_VNF} "mkdir -p ${DIR}"
    # Copy B's source code there
    scp ${B_FILES} ${B_VNF}:${DIR}
    # Make B and run it
    ssh ${B_VNF} "
        cd ${DIR};
        make ${B_TARGET};
        # Kill existing screen
        screen -list | grep ${SCREEN_NAME} | cut -d. -f1 | awk '{print $1}' | xargs kill 2>/dev/null;
        screen -L -Logfile ${B_TARGET}.log -dmS ${SCREEN_NAME} ./${B_TARGET} ${B_IP} ${B_PORT} ${C_IP} ${C_PORT}
        "

    # Check if B is running or not
    if [[ "$(ssh ${B_VNF} "screen -list | grep ${SCREEN_NAME}")" ]]; then
        return 0
    else
        log "Not able to start B"
        return 1
    fi
}

deploy_and_start_a () {
    log "Deploying and starting ${A_TARGET}"

    local SCREEN_NAME="libvnf-${TEST_NAME}-${A_TARGET}"
    # Dir to contain A's files
    local DIR="${TEST_DIR}/${A_TARGET}"
    ssh -t ${A_VNF} "mkdir -p ${DIR}"
    # Copy A's source code there
    scp ${A_FILES} ${A_VNF}:${DIR}
    # Make A and run it
    ssh ${A_VNF} "
        cd ${DIR};
        make ${A_TARGET};
        # Kill existing screen
        screen -list | grep ${SCREEN_NAME} | cut -d. -f1 | awk '{print $1}' | xargs kill 2>/dev/null;
        screen -L -Logfile ${A_TARGET}.log -dmS ${SCREEN_NAME} ./${A_TARGET} ${A_THREADS} ${A_DURATION} ${A_IP} ${A_PORT} ${B_IP} ${B_PORT}
        "

    # Check if A is running or not
    if [[ "$(ssh ${A_VNF} "screen -list | grep ${SCREEN_NAME}")" ]]; then
        return 0
    else
        log "Not able to start A"
        return 1
    fi
}


watch_report_and_clean () {
    log "Waiting for test to complete"
    # Wait for test to finish
    local PADDED_DURATION=$((A_DURATION+4))
    for timer in `seq 1 ${PADDED_DURATION}`;
    do
        local time_left=$((PADDED_DURATION-timer))
        printf '['
        printf '=%.0s' $(seq 1 ${timer})
        [[ ${time_left} -gt 0 ]] && printf ' %.0s' $(seq 1 ${time_left})
        printf ']'
        echo -ne " Time left: ${time_left}s \r"
        sleep 1
    done
    echo -ne '\n'

    # Retrieve useful data; here it is throughput
    log "Retrieving throughput"
    ssh -t yashsriram@10.129.131.78 "
    screen -S libvnf-${TEST_NAME}-${B_TARGET} -X at '#' stuff ^C
    screen -list | grep libvnf-${TEST_NAME} | cut -d. -f1 | awk '{print $1}' | xargs kill 2>/dev/null;
    "
    scp ${B_VNF}:"${TEST_DIR}/${B_TARGET}/${B_TARGET}.log" "${B_TARGET}.log"
    local LOG_TAIL=$(cat "${B_TARGET}.log" | tail -n 4)
    echo "${LOG_TAIL}"

    notify-send -a "libvnf-simulator" "${TEST_NAME} is complete" "${LOG_TAIL}"
}

log () {
    local msg_len=${#1}
    [[ ${msg_len} -eq 0 ]] && return 0

    local COLOR='\033[1;36m'
    local NOCOLOR='\033[0m'
    local decorator='========'

    echo -e "${COLOR}${decorator} $1${NOCOLOR}"

}

deploy_and_start_c || exit 1
[[ "$1" = "reinstall" ]] && install_libvnf_in_b_vnf
deploy_and_start_b || exit 1
deploy_and_start_a || exit 1
watch_report_and_clean || exit 1
