## What?
* redis_epoll_wrapper is a wrapper on redis server
* It runs on the VM with redis server. It connects locally with redis server.
* It acts as a server for persisting data/state of VNFs.
* libvnf can talk to it and make transactions of VNF state.
* Install Redis as given in [this manual](https://github.com/networkedsystemsIITB/NFV_LTE_EPC/blob/master/NFV_LTE_EPC-2.0/KeyValueStore/Docs/UserManual.pdf)
* Download hiredis-vip in the Redis/client/src folder from [here](https://github.com/vipshop/hiredis-vip) and do make

## How to run?
* Put the above files in Redis/client/src
* Specify the IP address in the DATASTORE_IP variable in redis_epoll_wrapper.cpp
* Run the following commands
* `mkdir build && cd build`
* `cmake ..`
* `make`
* This will create an executable named `redis_epoll_wrapper`
* Run it using `./redis_epoll_wrapper #portno` 
* This port number should be used as the datastore port in the VNF code.
