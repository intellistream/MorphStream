#!/bin/bash
set -e
source ../dir.sh || exit
cd ../../
mvn install -DskipTests
cd -