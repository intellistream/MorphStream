#!/bin/bash
# 加载环境变量
source Env/Cluster.env
read -p "Enter username: " username
read -p "Enter the name of the application: " appName
# 远程执行命令的函数
execute_remote() {
  local host=$1
  local command=$2
  echo "Executing on $host: $command"
  #ssh ${username}@$host "$command"
}
echo "Deploying"
# 在 driverHost 上执行 driver.sh
scp -r Env driver.sh ${username}@${driverHost}:~/ && \
execute_remote $driverHost "docker run --network host --env-file Env/Cluster.env --env-file Env/System.env --env-file Env/$appName.env --privileged --device=/dev/infiniband/ -it rtfaas:1.0 driver && rm -rf Env driver.sh"
# 在 databaseHost 上执行相应操作（示例：启动数据库）
execute_remote $databaseHost "bash database.sh"

# 遍历 workerHosts，在每个 worker 上执行 worker.sh
IFS=',' read -r -a hosts <<< "$workerHosts"
IFS=',' read -r -a ports <<< "$workerPorts"

for i in "${!hosts[@]}"; do
  workerHost=${hosts[$i]}
  workerPort=${ports[$i]}
  scp -r Env worker.sh ${username}@${workerHost}:~/ && \
  execute_remote $workerHost "docker run --network host --env-file Env/Cluster.env --env-file Env/System.env --env-file Env/$appName.env --privileged --device=/dev/infiniband/ -it rtfaas:1.0 worker $i && rm -rf Env worker.sh"
done

scp -r Env client.sh ${username}@${clientHost}:~/ && \
execute_remote $clientHost "docker run --network host --env-file Env/Cluster.env --env-file Env/System.env --env-file Env/$appName.env --privileged --device=/dev/infiniband/ -it rtfaas:1.0 client && rm -rf Env driver.sh"

