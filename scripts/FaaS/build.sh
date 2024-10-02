#!/bin/bash
set -e
source ../dir.sh || exit
cd ../../
echo "Building rtfaas"
mvn clean
mvn install -DskipTests
rm -rf DockerFiles/rtfaas.tar
echo "Building docker image"
docker build -t rtfaas:1.0 .
cd -
cd DockerFiles
docker save -o rtfaas.tar rtfaas:1.0
docker rmi rtfaas:1.0