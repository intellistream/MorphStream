#!/bin/bash -e
rm -dfr build
mkdir build && cd build 
cmake .. -DSTACK=KERNEL
make
sudo make install && echo "Done: libVNF built and installed."

cd ../vnf/naiveLoadBalancer && make clean && make b-kernel-static && make a && make c && echo "Done: LoadBalancer built."
