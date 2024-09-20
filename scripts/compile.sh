#!/bin/bash

cd ..
cd affinity/src/main/c
make
cd ../../../../
mvn clean install -Dmaven.test.skip=true
cd scripts
