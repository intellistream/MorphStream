#!/bin/bash -e

# Check system version. mtcp installation want kernel 4.5- and gcc7.5
if [ "$EUID" -ne 0 ]
  then echo "Please run as root"
  exit -1
fi

BASE_DIR=`pwd`
KEREL_VERSION=`uname -a 2>&1 | awk '{print $3}' | awk -F- '{print $1}'`
GCC_VERSION=`gcc --version | head -n 1 | awk '{print $4}'`
if [[ $KERNEL_VERSION != "4.15.0" || $GCC_VERSION != "7.5.0" ]]; then
	echo "Kernel version and gcc version not match, may cause compilation failed. Continue? [y|n]"
	read continue
	if [[ $continue != y ]]; then 
		exit 0
	fi
fi

# deb [arch=amd64] http://archive.ubuntu.com/ubuntu bionic main universe

DEPENDENCY="libdpdk-dev libnuma-dev librt-client-rest-perl libgmp-dev \
	linux-headers-$(uname -r) \
	build-essential \
	dpdk-kmods-dkms \
	gcc-7 \
	g++-7 \
	libsctp-dev libboost-all-dev"

apt-get install -y $DEPENDENCY

rm -dfr tmp && mkdir tmp && cd tmp 

if [[ -f automake_1.16.1-4ubuntu6_all.deb ]]; then 
else
	wget http://mirrors.kernel.org/ubuntu/pool/main/a/automake-1.16/automake_1.16.1-4ubuntu6_all.deb
fi
apt install ./automake_1.16.1-4ubuntu6_all.deb

# apt cmake is too old.
if [[ -f cmake-3.12.0-Linux-x86_64.sh ]]; then
else
	wget https://github.com/Kitware/CMake/releases/download/v3.12.0/cmake-3.12.0-Linux-x86_64.sh
fi
./cmake-3.12.0-Linux-x86_64.sh 

if [[ -f bin/cmake ]]; then
	CMAKE=`pwd`/bin/cmake
else
	echo "Cmake installation failed. Exit".
	exit -1
fi

echo "building mTCP.."

git clone https://github.com/mtcp-stack/mtcp/ && cd mtcp \
	&& git submodule init \
	&& git submodule update 

export RTE_TARGET=x86_64-native-linuxapp-clang
export RTE_SDK=`pwd`/dpdk

ln -s /usr/lib/python3 /usr/lib/python

# Shall be mannually operated.
./setup_mtcp_dpdk_env.sh dpdk
# cat > ./setup_mtcp_dpdk_env.sh dpdk << EOF
# 15
# 18
# 22 64
# 24
# 35
# EOF

cd .. && ./configure --with-dpdk-lib=$RTE_SDK/$RTE_TARGET
make

if [[ -f include/mtcp_api.h ]]; then 
	echo "mTCP building done."
fi

# Requiring manually installation of spdlog.
cd tmp
git clone https://github.com/gabime/spdlog.git
cd spdlog && $CMAKE . && make

# apt spdlog version is too old.
if [[ -f include/spdlog.h ]]; then
	mkdir $BASE_DIR/include/spdlog
	cp include/* $BASE_DIR/include/spdlog
else
	echo "spdlog build failed."
	exit -1
fi

if [[ -x java ]]; then
else
	echo "java not installed."
	exit -1
fi

# Check and install JAVA
if [[ $((echo $JAVA_HOME)) -z ]]; then
	export JAVA_HOME=$((java -XshowSettings:properties -version 2>&1 | grep java.home | awk '{print $3}'))
fi

if [[ -f $JAVA_HOME/include/jni.h ]]; then
	echo "Jni.h not found"
	exit -1
else
	echo "JNI in $JAVA_HOME/include/jni.h"
fi

cd $BASE_DIR && mkdir build && cd build
$CMAKE .. -DSTACK=KERNEL_BYPASS

make

echo "Installation done."