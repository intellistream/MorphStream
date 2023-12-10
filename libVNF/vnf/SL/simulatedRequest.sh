#!/bin/bash

IP="127.0.0.1"
Port=9090

cat << EOF | nc "$IP" "$Port"
accounts:2988,bookEntries:2988;depositAmount:6447;depositAmount:int;deposit;false
EOF