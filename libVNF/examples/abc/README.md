# ABC Network
* ABC is a simple network model for demonstrating **libvnf** usage
* There are three network functions in this example namely A, B & C
* The nodes are connected as shown below
    * A --- B --- C
* The packet flow is as follows
    * Node A sends request to Node B
    * Node B contacts Node C to fetch some data
    * Node C replies back to Node B
    * Node B then replies back to A with the response from Node C
    * And this repeats
* This models a common client-server-database architecture
* Only node B is built using **libvnf**

# Variants
* Each of the directories represent a unique variant of ABC
* **layman** variant assumes that there is only a single request on a single connection
* **scmr** variant assumes that there can be multiple requests on a connection and can have multiple outstanding requests on a connection
* sctp-http versions use sctp protocol between A-B and http between B-C

# How to run?
## Automatically
* `simulation_parameters.sh` contains parameters which can be changed as necessary
* Using those paramters `simulator.sh` deploys and starts above network functions in order, waits for simulation to end and finally retrieves the average throughput of the run
* By default it does not install libvnf at the B's VNF, but it can be done by passing `reinstall` argument to `simulator.sh`
* Usages
  * `./simulator.sh`
  * `./simulator.sh reinstall`

## Manually
### Compilation
* Compile C node using
    * `make c`
* Install **libvnf** by following steps in [README.md](../../../README.md) at the project root
* Make sure to install `KERNEL` implementation of libvnf
* To compile node B
    * with static linking to libvnf run `make b-kernel-static`
    * with dynamic linking to libvnf run `make b-kernel-dynamic`
       * In this case make sure to add /usr/local/lib to $LD_LIBRARY_PATH
       * This can be done by executing `export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH`
    * When using kernel bypass stack option (tested for the layman version) add appropriate paths in [Makefile](layman/Makefile) as below:
       * For mTCP + netmap setup, change the mTCP path in line 15
       * For mTCP + DPDK setup, change the following:
           * line 10 DPDK=1 
           * line 12 NETMAP=0
           * line 15, approriate mTCP path
           * line 29, DPDK_INC=$(MTCP_P_FLD)/dpdk/include
           * line 40, DPDK_LIB=$(MTCP_P_FLD)/dpdk/lib/
           * line 42, DPDK_MACHINE_FLAGS = $(shell cat $(MTCP_P_FLD)/dpdk/include/cflags.txt)
           * line 51, DPDK_LIB_FLAGS = $(shell cat $(MTCP_P_FLD)/dpdk/lib/ldflags.txt)
    * When using kernel bypass stack, following changes are required in [server.conf](layman/server.conf)
        * set num_cores (line 17) equal to number of cores
        * For mTCP + netmap setup, set the netmap interface in line 37
        * For mTCP + DPDK setup, change the following:
            * comment line 6, uncomment line 7
            * uncomment line 31, comment line 37
* Compile A node using
    * `make a`

### Misc
* Run
    * `ulimit -n 65535` as root
    * `echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse` as root

### Execution
* Start C node using `./c <c-ip> <c-port>`
* Start B node using `./b-kernel-{static/dynamic} <b-ip> <b-port> <c-ip> <c-port>`
* Start A node using `./a <no-threads> <no-seconds> <a-ip> <a-port> <b-ip> <b-port>`, where
