#!/bin/bash

IP="127.0.0.1"
Port=9090

cat << EOF | nc "$IP" "$Port"
13930,true,HTTP,1,false
EOF