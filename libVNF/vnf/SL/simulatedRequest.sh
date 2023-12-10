#!/bin/bash

IP="127.0.0.1"
Port=9090

cat << EOF | nc "$IP" "$Port"
deposit
EOF