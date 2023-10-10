# VNF based on libVNF

* **libvnf** is a library to easily build custom, scalable, high-performance, virtual network functions
* This is an implementation of paper [libVNF: Building Virtual Network Functions Made Easy](https://www.cse.iitb.ac.in/~mythili/research/papers/2018-libvnf-socc.pdf) accepted in SoCC 2018
* Refer to [release-socc](https://github.com/networkedsystemsIITB/libVNF/tree/release-socc) branch for the above version

This repository serves as a set of VNFs based on libVNF for research purpose.

My works:
- A simple naiveLoadBalancer, modified from abc example. with detailed comment.

# libvnf

### Features
* Written entirely in C++
* Aims to reduce lines of code without compromising on performance
* Has a non-blocking event-driven architecture
* Supports building of transport layer end-point VNFs as well as L2/L3 VNFs
* Supports kernel, kernel-bypass and L3 stacks \[chosen at compile time\]
* APIs are stack agnostic

# Navigating Repo
* The headers are in [include](include) directory
* The implementations are in [src](src) directory
* Example VNFs that use **libvnf** are provided in [examples](examples) dir
* Custom dependencies are listed in [dependencies](dependencies) dir

# Dependencies
* Before installation, the following dependencies need to be satisfied
    * cmake version >= 3.5.0
    * spdlog (https://github.com/gabime/spdlog)
    * boost (libboost-all-dev)
    * Kernel Stack
        * Install libsctp using `sudo apt install libsctp-dev`
        * While compiling the VNF, "-lsctp" flag needs to be used
    * Kernel-bypass stack
        * mTCP
        * netmap + vale switch
        * dpdk
        * numa (libnuma-dev)
        * Installation instructions can be found at [dependencies/kernel_bypass_stack](dependencies/kernel_bypass_stack)
    * Layer3 VNF
        * Netmap uses vale as the software switch. Follow steps (1-4) from [here](https://github.com/networkedsystemsIITB/Modified_mTCP/blob/master/mTCP_over_Netmap/docs/netmap_docs/user_manual.pdf)


# Installation
* **libvnf** can be installed directly into the system using cmake
* In [project root directory](.) execute the following
    * `mkdir build`
    * `cd build`
    * `cmake .. -DSTACK=KERNEL` or `cmake .. -DSTACK=KERNEL_BYPASS` or `cmake .. -DSTACK=L3VNF` 
    * `make`
    * `sudo make install`
* This creates and installs
    * a shared object (.so) version for of libvnf dynamic linking
    * an archive (.a) version of libvnf for static linking
* Dynamically linkable version of library will be named **libvnf-{kernel/kernelbypass/l3}-dynamic.so**
* Statically linkable version will be named **libvnf-{kernel/kernelbypass/l3}-static.a**
* CMake caches the options passed to it, so once passed there is no need to pass the option (-DSTACK=...) from second time
* If you want to change the stack, delete all files in `build` dir and run cmake again with required stack

# How to use?
* Use `#include <libvnf/core.hpp>` header
* Refer to the [abc example](examples/abc) for a gentle introduction to APIs
* For compilation of your VNF you can just use -lvnf-kernel-static if you are using library compiled for kernel stack with static linkage. The other variants of linkage include flags -lvnf-{kernel,kernel-bypass,kernel-l3}-{static,dynamic}.

# Optional Configuration
* While building I/O intensive applications on kernel stack run
    * `ulimit -n 65535` as root
    * `echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse` as root

# Notes on Optimization
* To achieve maximum possible performance make sure to use `-O3` when compiling your VNF

# Collaborators
1. [Priyanka Naik](https://www.cse.iitb.ac.in/~ppnaik/)
1. [Yashasvi Sriram Patkuri](https://github.com/Yashasvi-Sriram)
1. [Sai Sandeep Moparthi](https://github.com/sandeep-END)
1. [Akash Kanase](https://in.linkedin.com/in/akashkanase)
1. [Trishal Patel](https://www.cse.iitb.ac.in/~trishal/)
1. [Sagar Tikore](https://www.cse.iitb.ac.in/~sagart/)
1. [Vaishali Jhalani](https://www.cse.iitb.ac.in/~vaishali/)
1. [Prof. Mythili Vutukuru](https://www.cse.iitb.ac.in/~mythili/)
