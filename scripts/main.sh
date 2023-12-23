#!/bin/bash -e

# ------------------------------------------------------------------
# [Kailian Jacy] DB4NFV running script
#          Entry for running and testing the system.
# ------------------------------------------------------------------

# Stages: Setup, Compile, Run.

# DIRS
SCRIPT_DIR=$(pwd)
BUILD_DIR=$SCRIPT_DIR/build
TMP_DIR=$SCRIPT_DIR/tmp
LIBVNF_DIR=$SCRIPT_DIR/libVNF
MORPH_DIR=$SCRIPT_DIR/morphStream

PROJ_DIR=$MORPH_DIR/morph-core/src/main/java
INTERFACE_FILE=$PROJ_DIR/intellistream/morphstream/util/libVNFFrontend/NativeInterface.java
HEADER=$LIBVNF_DIR/include
HEADER_INSTALL=/usr/local/include/libvnf/

# Executable
CMAKE=$TMP_DIR/cmake/bin/cmake
if [ -x "$CMAKE" ]; then
	:
else
	sudo apt-get install -y cmake && CMAKE=cmake
fi
echo "Using cmake: $CMAKE"

# If you let script make the vm.
VM_ISO=$TMP_DIR/ubuntu-18.04.1-live-server-amd64.iso
ISO_URL="https://releases.ubuntu.com/18.04/ubuntu-18.04.6-live-server-amd64.iso"
VM_NAME="DB4NFV"
DISK_SIZE=64
MEMORY_SIZE=8192
CPU_CORES=4
VM_DISK=$TMP_DIR/$VM_NAME.raw

# Command line
SCRIPT='./main'
NEWVM='--new_vm'
RUN='--run'
HELP='--help'
COMPILE_JNI='--compile_jni'
COMPILE='--compile'
KERNEL_BYPASS="--KENREL_BYPASS" 
KERNEL_STACK="--KERNEL_STACK"
MORPH="--MORPH_STREAM"

# Compile Example VNF options
EXAMPLE="--EXAMPLE_VNF"
VNF_PATH="$LIBVNF_DIR/vnf/SL"

USAGE="$SCRIPT entry for running DB4NFV \n\t $NEWVM start a new vm for suitable kernel to run the system. \n\t $RUN [$KERNEL_STACK|$KERNEL_BYPASS] to run the compiled system. \n\t $COMPILE [$KERNEL_STACK|$KERNEL_BYPASS] to set up the environment and compile. "

compile_libVNF(){
	# TODO. check if includes has been there.
	# TODO. If needs kernel_bypass. remind to use kernel_bypass
	if [ -d $TMP ]; then 
		:
	else 
		mkdir $TMP
	fi

	if [ $# -ge 2 ] && [[ $2 == "$KERNEL_STACK" ]]; then
		rm -dfr "$BUILD_DIR" &> /dev/null || true
		mkdir "$BUILD_DIR" && cd "$BUILD_DIR"
		$CMAKE "$LIBVNF_DIR" -DSTACK=KERNEL \
			-DBACKEND_MORPH=True \
			-DCMAKE_BUILD_TYPE=Debug \
			-DJAVA_JNI_INTERFACE="$INTERFACE_FILE"
		rm -dfr $HEADER_INSTALL 
		mkdir $HEADER_INSTALL && cp "$HEADER/core.hpp" "$HEADER_INSTALL/"
		make install && echo "Done: libVNF built and installed." 
		exit 0
	elif [ $# -ge 2 ] && [[ $2 == "$KERNEL_BYPASS" ]]; then 
		local 
	fi
	exit 0
}

compile_jni_header(){
	cd "$MORPH_DIR"
	# TODO. Copy header file to dir.
	javac -cp .:"$PROJ_DIR" -h "$HEADER" "$INTERFACE_FILE"
	echo "JNI Header generated at $HEADER"
}

compile_morphStream(){
	compile_jni_header
	find "$MORPH_DIR" -name "*jar" -print | xargs rm && rm -dfr ~/.m2/repository/intellistream/
	# find "$MORPH_DIR" -name 'target' -type d -print | xargs sudo rm -dfr
	# dest=("common" "core" "web" "clients")

	# for i in "${dest[@]}" 
	# do 
	# 	cd $MORPH_DIR/morph-$i 
	# 	sudo mvn clean install
	# 	cd ..
	# done

	cd "$MORPH_DIR" && mvn install && cd -

	# TODO. Copy header file to dir.
}

run(){
	# Check if setup_kernel_bypass_stack and compiled. If not, do.
	# Else just run the corresponding stack.
	:
}

error_exit(){
	echo "failed to compile. exit." 
	exit 1
}

ynSelect(){
	echo $1
	read res 
	if [[ $res == "y" ]]; then
		return 0
	elif [[ $res == "n" ]]; then 
		return 1
	else
		return "$(ynSelect "$1")"
	fi
}

check_system(){
	# Check the environment.
	if [[ $(uname -r | awk -F'-' '{print $1}') != "4.15.0" ]]; then 
		echo "Your system is not matching required kernel version 4.15.0-213."
		echo "You are suggested to create a virtual machine.  Configure the script and use $SCRIPT $STARTVM to start." 
		install_vm=$(ynSelect "Install qualified ubuntu vm now? [y/n]")
		if [[ $install_vm == 0 ]]; then
			new_vm
		else
			exit 0
		fi
	fi
	return 0
}

new_vm(){
	echo "creating new vm.."
	if [ -f $VM_ISO ]; then
		wget $ISO_URL -O $VM_ISO
	fi

	# Check if the VM is already running
	running_machine=$(ps -ef | grep "qemu" | grep "\-name $VM_NAME")
	if [[ -z $running_machine ]]; then
		:
	else
		echo "VM is already running."
		exit 1
	fi


	qemu-img create -f raw $VM_DISK ${DISK_SIZE}G
	
	# Start the VM
	qemu-system-x86_64 -name "$VM_NAME" \
		-machine type=q35,accel=kvm \
		-cpu host -smp $CPU_CORES \
		-m $MEMORY_SIZE \
		-drive file="$VM_DISK",size=$DISK_SIZE,format=raw \
		-cdrom "$ISO_PATH" \
		-nographic \
		-boot order=d \
		-vnc :0 -k en-us -vga none \
		-netdev user,id=net0 -device virtio-net-pci,netdev=net0 \
		-usb -device usb-tablet \
	&& echo "create success. use vnc to get in and install." \
	&& return 0

	echo "new vm installation failed. exit."  && return 1
}

setup_normal() {
	# Requiring manually installation of spdlog.
	if [ -d "$HEADER/spdlog" ]; then
		:
	elif [ -d "/usr/include/spdlog" ]; then
		:
	else
		cd "$TMP_DIR"
		git clone https://github.com/gabime/spdlog.git
		cd spdlog && $CMAKE . && make -j
		# apt spdlog version is too old.
		if [[ -f include/spdlog/spdlog.h ]]; then
			cp -r include/spdlog "$HEADER/"
			cp -r include/spdlog "/usr/include"
		else
			echo "spdlog build failed."
			exit 1
		fi
	fi

	if command -v java >/dev/null 2>&1 && java -version >/dev/null 2>&1; then
		:
	else
		echo "java not installed."
		exit 1
	fi

	# Check and install JAVA
	if [[ -z $(echo $JAVA_HOME) ]]; then
		export JAVA_HOME=$(java -XshowSettings:properties -version 2>&1 | grep java.home | awk '{print $3}')
	fi

	if [[ -f $JAVA_HOME/include/jni.h ]]; then
		echo "Jni.h not found"
		exit 1
	else
		echo "JNI in $JAVA_HOME/include/jni.h"
	fi
}

compile_example_vnf() {
	setup_normal || error_exit
	echo "Compiling Example VNF: $VNF_PATH"
	cd "$VNF_PATH"
	make clean
	make kernel-dynamic JAVA_HOME="$JAVA_HOME" || error_exit
	# echo "Compiling Done"
}

# TODO.
setup_kernel_stack(){
	setup_normal || error_exit
}

# TODO.
setup_kernel_bypass_stack(){
	setup_normal || error_exit
	# Setup gcc and use gcc version	
	GCC_VERSION=$(gcc --version | head -n 1 | awk '{print $4}')
	local DEPENDENCY="libdpdk-dev libnuma-dev librt-client-rest-perl libgmp-dev \
	linux-headers-$(uname -r) \
	build-essential \
	gcc-7 \
	g++-7 \
	dpdk-kmods-dkms \
	libsctp-dev libboost-all-dev"

	if [[ $GCC_VERSION != "7.5.0" ]]; then 
	 	# TODO
		echo "GCC version is not 7.5.0. exit"
		exit 1
	fi

	apt-get install -y $DEPENDENCY

	rm -dfr $TMP_DIR && mkdir $TMP_DIR && cd $TMP_DIR 

	# TODO.
	if [[ -f automake_1.16.1-4ubuntu6_all.deb ]]; then 
		:
	else
		wget http://mirrors.kernel.org/ubuntu/pool/main/a/automake-1.16/automake_1.16.1-4ubuntu6_all.deb
		apt install ./automake_1.16.1-4ubuntu6_all.deb
	fi

	# apt cmake is too old. We need a new one here.
	# TODO.
	if [[ -f $CMAKE ]]; then
		:
	else
		wget https://github.com/Kitware/CMake/releases/download/v3.12.0/cmake-3.12.0-Linux-x86_64.sh
		./cmake-3.12.0-Linux-x86_64.sh 
	fi

	if [[ -f $CMAKE ]]; then
		:
	else
		echo "Cmake installation failed. Exit".
		exit 1
	fi

	echo "building mTCP.."

	git clone https://github.com/mtcp-stack/mtcp/ && cd mtcp \
		&& git submodule init \
		&& git submodule update 

	export RTE_TARGET=x86_64-native-linuxapp-clang
	export RTE_SDK=$(pwd)/dpdk

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

	cd .. && ./configure --with-dpdk-lib="$RTE_SDK/$RTE_TARGET"
	make

	if [[ -f include/mtcp_api.h ]]; then 
		echo "mTCP building done."
	fi
	echo "Installation done. Use $SCRIPT $RUN to start."
}

# Entry route.
if [ $# == 0 ] ; then
    echo "$USAGE"
    exit 1;
fi

if [ "$EUID" -ne 0 ]
  then echo "Please run as root"
  exit 1;
fi

case $1 in 
	$HELP)
		echo -e "$USAGE"
		exit 0
		;;
	$NEWVM)
		check_system && echo "Your system already qualified. Use $SCRIPT $RUN to run."
		exit 0
		;;
	$RUN)
		echo "starting compiled DB4NFV system."
		echo "to be done later."
		check_system
		exit 0
		;;
	$COMPILE_JNI)
		echo "starting compiling JNI header."
		check_system
		compile_jni_header
		exit 0
		;;
	$COMPILE)
		echo "starting compilation.."
		if [ $# -ge 2 ] && [[ $2 == $KENREL_BYPASS ]]; then
			check_system || error_exit
			IS_KENREL_BYPASS=true
			setup_kernel_bypass_stack
		elif [ $# -ge 2 ] && [[ $2 == $KERNEL_STACK ]]; then
			IS_KENREL_BYPASS=false
			setup_kernel_stack
		elif [ $# -ge 2 ] && [[ $2 == $EXAMPLE ]]; then
			compile_example_vnf || error_exit
			rm "$TMP_DIR/$(basename "$VNF_PATH")-kernel-dynamic.so" &>/dev/null || true
			mv "$VNF_PATH/kernel-dynamic" "$TMP_DIR/$(basename "$VNF_PATH")-kernel-dynamic.so"
			exit 0
		elif [ $# -ge 2 ] && [[ $2 == "$MORPH" ]]; then
			compile_morphStream
			exit 1
		else 
			echo $USAGE 
			echo "failed. exit." 
			exit 1
		fi
		compile_jni_header
		compile_libVNF "$@" || error_exit
		echo "Setup done"
		exit 0;
		;;
esac
