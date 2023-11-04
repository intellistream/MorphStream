#!/bin/bash -e

rm -dfr build &>/dev/null || true
mkdir build && cd build	

cmake .. -DSTACK=KERNEL \
	-DBACKEND_MORPH=True \
	-DCMAKE_BUILD_TYPE=Debug

make -j4 
sudo make install

echo "Compilation and installation done."