#!/bin/bash
serviceName=$1
appName=$2
workerId=$3

cd FaaS
docker load -i DockerFiles/rtfaas.tar
if [ "$serviceName" = "driver" ]; then
    docker run --rm --network host --env-file Env/Cluster.env --env-file Env/System.env --env-file Env/$appName.env --env-file Env/Database.env --privileged --device=/dev/infiniband/ -it rtfaas:1.0 driver
elif [ "$serviceName" = "worker" ]; then
    docker run --rm --network host --env-file Env/Cluster.env --env-file Env/System.env --env-file Env/$appName.env --env-file Env/Database.env --privileged --device=/dev/infiniband/ -it rtfaas:1.0 worker $workerId
elif [ "$serviceName" = "client" ]; then
    docker run --rm --network host --env-file Env/Cluster.env --env-file Env/System.env --env-file Env/$appName.env --env-file Env/Database.env --privileged --device=/dev/infiniband/ -it rtfaas:1.0 client
else
    echo "No valid argument provided. Please pass 'driver' or 'worker' or 'client' ."
    exit 1
fi

cd -
#rm -rf FaaS
docker rmi rtfaas:1.0
