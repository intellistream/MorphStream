# 检查传递的参数
if [ "$1" = "driver" ]; then
    echo "Starting the driver"
    /rtfaas/driver.sh
elif [ "$1" = "worker" ]; then
    echo "Starting the worker"
    /rtfaas/worker.sh $2
elif [ "$1" = "client" ]; then
    echo "Starting the clients"
    /rtfaas/client.sh
elif [ "$1" = "database" ]; then
    echo "Init the database"
    /rtfaas/database.sh
else
    echo "No valid argument provided. Please pass 'driver' or 'worker' or 'client' ."
    exit 1
database