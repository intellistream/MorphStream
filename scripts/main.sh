#!/bin/bash -e

# ------------------------------------------------------------------
# [Kailian Jacy] DB4NFV running script
#          Entry for running and testing the system.
# ------------------------------------------------------------------

# Stages: Setup, Compile, Run.

# DIRS
PROJ_DIR=$(pwd)
BUILD_DIR=$PROJ_DIR/build
TMP_DIR=$PROJ_DIR/tmp
if [ -d $TMP_DIR ]; then 
	:
else 
	mkdir $TMP_DIR
fi
LIBVNF_DIR=$PROJ_DIR/libVNF
SCRIPTS_DIR=$PROJ_DIR/scripts
MORPH_DIR=$PROJ_DIR/morphStream
RESULT_DIR=$PROJ_DIR/measurement

PROJ_DIR=$MORPH_DIR/morph-core/src/main/java
INTERFACE_FILE=$PROJ_DIR/intellistream/morphstream/util/libVNFFrontend/NativeInterface.java
HEADER=$LIBVNF_DIR/include
HEADER_INSTALL=/usr/local/include/libvnf/

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# Executable
CMAKE=$TMP_DIR/cmake/bin/cmake
JAR=$MORPH_DIR/morph-clients/morph-clients-0.1.jar
if [ -x "$CMAKE" ]; then
	:
else
	sudo apt-get install -y cmake && CMAKE=cmake
fi
echo "Using cmake: $CMAKE"

# Globals
DEBUG=true
IS_KENREL_BYPASS=false

# If you let script make the vm.
ISO_URL="https://releases.ubuntu.com/18.04/ubuntu-18.04.6-live-server-amd64.iso"
VM_NAME="DB4NFV"
DISK_SIZE=64
MEMORY_SIZE=8192
CPU_CORES=24
VM_ISO_DIR=/var/lib/libvirt/images/DB4NFV_TESTER
VM_ISO=ubuntu-18.04.1-live-server-amd64.iso
VM_DISK=$TMP_DIR/$VM_NAME.raw

# Command line
SCRIPT='./main'
NEWVM='--new_vm'
RUN='--run'
HELP='--help'
COMPILE_JNI='--compile_jni'
COMPILE='--compile'
KERNEL_BYPASS="--KERNEL_BYPASS" 
KERNEL_STACK="--KERNEL_STACK"
MORPH="--MORPH_STREAM"
TEST="--TEST"
PLOT="--PLOT"

# Compile Example VNF options
EXAMPLE="--EXAMPLE_VNF"
VNF_PATH="$LIBVNF_DIR/vnf/SL"

USAGE="$SCRIPT entry for running DB4NFV \n\t $NEWVM start a new vm for suitable kernel to run the system. \n\t $RUN [$KERNEL_STACK|$KERNEL_BYPASS] to run the compiled system. \n\t $COMPILE [$KERNEL_STACK|$KERNEL_BYPASS] to set up the environment and compile. "

compile_libVNF(){
	# TODO. check if includes has been there.
	# TODO. If needs kernel_bypass. remind to use kernel_bypass

	if [ $# -ge 2 ] && [[ $2 == "$KERNEL_STACK" ]]; then
		rm -dfr "$BUILD_DIR" &> /dev/null || true
		mkdir "$BUILD_DIR" && cd "$BUILD_DIR"
		if [[ $DEBUG == true ]]; then 
			$CMAKE "$LIBVNF_DIR" -DSTACK=KERNEL \
				-DCMAKE_BUILD_TYPE=Debug  \
				-DJAVA_JNI_INTERFACE="$INTERFACE_FILE"
		else 
			$CMAKE "$LIBVNF_DIR" -DSTACK=KERNEL \
				-DCMAKE_BUILD_TYPE=Release \
				-DJAVA_JNI_INTERFACE="$INTERFACE_FILE"
		fi
		rm -dfr $HEADER_INSTALL 
		mkdir $HEADER_INSTALL && cp "$HEADER/core.hpp" "$HEADER_INSTALL/"
		make install -j4 && echo "Done: libVNF built and installed." 
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

run_and_test_with_graph_output(){
	apt-get install -y tmux python3-pip libjpeg-dev zlib1g-dev | grep -V 'is already the newest version'

	if [ -d "$RESULT_DIR" ]; then 
		:
	else 
		mkdir "$RESULT_DIR"
	fi
	
	if [ -f "$JAR" ]; then
		:
	else 
		compile_morphStream || error_exit
	fi

	( /usr/bin/java -jar "$JAR" ) | { 
		grep this_is_a_report | awk '{ print $4 }' > "$RESULT_DIR/raw_record.csv"
	}

	echo "Result record sample:"
	head -n 10 "$RESULT_DIR/raw_record.csv"

	plot || error_exit
}

plot(){
	# Draw graph.
	echo "Generating graph."
	pip3 install pandas matplotlib seaborn | grep -v 'already satisfied'
	python3 "$SCRIPTS_DIR/graph.py" "$RESULT_DIR/raw_record.csv" "$RESULT_DIR"

	echo "Graph Generated."
}

compile_morphStream(){
	compile_jni_header
	# find "$MORPH_DIR" -name 'target' -type d -print | xargs sudo rm -dfr
	# dest=("common" "core" "web" "clients")

	# for i in "${dest[@]}" 
	# do 
	# 	cd $MORPH_DIR/morph-$i 
	# 	sudo mvn clean install
	# 	cd ..
	# done
	mv "$JAR" "$JAR.bak"

	cd "$MORPH_DIR" && mvn clean install \
		&& cd morph-clients \
		&& mvn clean install

	ls target/morph-clients-0.1.jar
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
	apt install -y qemu-kvm libvirt-clients libvirt-daemon-system virtinst

	mkdir -p $VM_ISO_DIR

	if [ -f $VM_ISO_DIR/$VM_ISO ]; then 
		:
	else
		wget $ISO_URL -O $VM_ISO_DIR/$VM_ISO
	fi

	# Check if the VM is already running
	running_machine=$(virsh list --all | grep $VM_NAME)
	if [[ -z $running_machine ]]; then
		:
	else
		echo "VM is already created."
		exit 1
	fi


	qemu-img create -f raw $VM_DISK ${DISK_SIZE}G

	# Start the VM
 	virt-install \
		  --name "$VM_NAME" \
		  --memory $MEMORY_SIZE \
		  --vcpus $CPU_CORES \
		  --boot uefi \
		  --disk path="$VM_DISK",format=raw \
		  --virt-type kvm \
	          --os-type linux \
		  --os-variant "ubuntu18.04" \
		  --network bridge=vmbr0,model=e1000 \
		  --graphics vnc \
		  --cdrom "$VM_ISO_DIR/$VM_ISO"  || error_exit	
		  # --disk path="$VM_DISK",size=$DISK_SIZE \
	          # --console pty,target_type=serial \
		  # --extra-args 'console=ttyS0,115200n8 serial'  \
	
	echo "create success. use vnc to get in and install." \
	&& echo "Running VM:" && virsh list --all \
	&& return 0

	echo "new vm installation failed. exit."  && return 1
}

install_java(){
	echo "installing java 8.."
	apt-get install openjdk-8-jdk -y
}

setup_normal() {
	apt-get install -y libjsoncpp-dev

	# Requiring manually installation of spdlog.
	if [ -d "$HEADER/spdlog" ]; then
		:
	elif [ -d "/usr/include/spdlog" ]; then
		:
	else
		cd "$TMP_DIR"
		git clone https://github.com/gabime/spdlog.git
		cd spdlog && $CMAKE . && make -j4
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
		java_install=$(ynSelect "Install openjdk-8 now? [y/n]")
		if [[ $java_install == 0 ]]; then
			install_java
		else
			exit 0
		fi
		exit 1
	fi

	# Check JAVA
	# if [[ -z $(echo $JAVA_HOME) ]]; then
	# 	export JAVA_HOME=$(java -XshowSettings:properties -version 2>&1 | grep java.home | awk '{print $3}')
	# fi

	if [[ -f $JAVA_HOME/include/jni.h ]]; then
		echo "JNI in $JAVA_HOME/include/jni.h"
	else
		echo "Jni.h not found"
		exit 1
	fi
}

compile_example_vnf() {
	setup_normal || error_exit
	echo "Compiling Example VNF: $VNF_PATH"
	cd "$VNF_PATH"
	make clean
	if [[ $IS_KENREL_BYPASS == true ]]; then 
		 echo "unfinished." && exit 1
		 make kernel_bypass-dynamic JAVA_HOME="$JAVA_HOME" DEBUG=$DEBUG || error_exit
	else
		 make kernel-dynamic JAVA_HOME="$JAVA_HOME" DEBUG=$DEBUG || error_exit
	fi
}

# TODO.
setup_kernel_stack(){
	setup_normal || error_exit
}

# TODO.
setup_kernel_bypass_stack(){
	cd $TMP_DIR

	setup_normal || error_exit
	# Setup gcc and use gcc version	
	GCC_VERSION=$(gcc --version | head -n 1 | awk '{print $4}')
	local DEPENDENCY="libdpdk-dev libnuma-dev librt-client-rest-perl libgmp-dev \
	linux-headers-$(uname -r) \
	build-essential \
	gcc-7 \
	g++-7 \
	dpdk-igb-uio-dkms \
	libsctp-dev libboost-all-dev"

	if [[ $GCC_VERSION != "7.5.0" ]]; then 
	 	# TODO
		echo "GCC version is not 7.5.0. exit"
		exit 1
	fi

	apt-get install -y $DEPENDENCY

	# rm -dfr $TMP_DIR && mkdir $TMP_DIR && cd $TMP_DIR 

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
		chmod 700 ./cmake-3.12.0-Linux-x86_64.sh 
		cat >> ./cmake-3.12.0-Linux-x86_64.sh  << EOF
y
y
EOF
		mv ./cmake-3.12.0-Linux-x86_64 cmake
	fi

	if [[ -f $CMAKE ]]; then
		:
	else
		echo "Cmake installation failed. Exit".
		exit 1
	fi

	echo "building mTCP.."


	if [[ -d mtcp ]]; then 
		:
	else 
		git clone https://github.com/mtcp-stack/mtcp/ || true 

	fi

	cd mtcp \
		&& GIT_SSL_NO_VERIFY=1 git submodule init \
		&& GIT_SSL_NO_VERIFY=1 git submodule update 

	export RTE_TARGET=x86_64-native-linuxapp-gcc
	export RTE_SDK=$(pwd)/dpdk

	ln -s /usr/lib/python3 /usr/lib/python &>/dev/null || true

	# Shall be mannually operated.
	./setup_mtcp_dpdk_env.sh dpdk
	# cat >> "./setup_mtcp_dpdk_env.sh dpdk" << EOF
# 15
# 18
# 22 
# 64
# 24
# 35
# y
# EOF

	./configure --with-dpdk-lib="$RTE_SDK/$RTE_TARGET"
	make

	if [[ -f include/mtcp_api.h ]]; then 
		echo "mTCP building done."
	fi
	echo "Installation done. Use $SCRIPT $RUN to start."
}

# Entry route.
if [ $# == 0 ] ; then
    echo -e "$USAGE"
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
		new_vm || error_exit
		exit 0
		;;
	$TEST)
		run_and_test_with_graph_output || error_exit
		exit 0
		;;
	$PLOT)
		plot || error_exit
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
		if [ $# -ge 2 ] && [[ $2 == $KERNEL_BYPASS ]]; then
			check_system || error_exit
			export IS_KENREL_BYPASS=true
			setup_kernel_bypass_stack
			if [ $# -ge 3 ] && [[ $3 == $EXAMPLE ]]; then
				compile_example_vnf || error_exit
				rm "$TMP_DIR/$(basename "$VNF_PATH")-kernel_bypass-dynamic.so" &>/dev/null || true
				mv "$VNF_PATH/kernel_bypass-dynamic" "$TMP_DIR/$(basename "$VNF_PATH")-kernel_bypass-dynamic.so"
				exit 0
			fi
		elif [ $# -ge 2 ] && [[ $2 == $KERNEL_STACK ]]; then
			export IS_KENREL_BYPASS=false
			setup_kernel_stack
			if [ $# -ge 3 ] && [[ $3 == $EXAMPLE ]]; then
				compile_example_vnf || error_exit
				rm "$TMP_DIR/$(basename "$VNF_PATH")-kernel-dynamic.so" &>/dev/null || true
				mv "$VNF_PATH/kernel-dynamic" "$TMP_DIR/$(basename "$VNF_PATH")-kernel-dynamic.so"
				exit 0
			fi
		elif [ $# -ge 2 ] && [[ $2 == "$MORPH" ]]; then
			compile_morphStream
			exit 1
		else 
			echo -e $USAGE 
			echo "failed. exit." 
			exit 1
		fi
		compile_jni_header
		compile_libVNF "$@" || error_exit
		echo "Setup done"
		exit 0;
		;;
esac
