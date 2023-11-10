#!/bin/bash -e

PWD=/home/kailian/MorphStream

# libVNF compile
BUILDDIR=$PWD/libVNF/build

# Compile morphStream
PROJ_DIR=$PWD/morphStream/morph-clients/src/main/java
INTERFACE_FILE=$PROJ_DIR/cli/libVNFFrontend/Interface.java
HEADER=$PWD/libVNF/include

# Tools to be used.
CMAKE=~/cmake-3.12.0-Linux-x86_64/bin/cmake

if [[ $1 == "morph_all" ]]; then 
	cd "~/MorphStream"
	mvn -fn pacakge -Dmaven.test.failure.ignore=true
	exit 0
fi

if [[ $1 == "morph_jni" ]]; then 
	javac -cp .:$PROJ_DIR -h $HEADER $INTERFACE_FILE
	exit 0
fi

if [[ $1 == "libVNF" ]]; then 
	rm -dfr $BUILDDIR &> /dev/null || true
	mkdir $BUILDDIR && cd $BUILDDIR
	$CMAKE .. -DSTACK=KERNEL \
		-DBACKEND_MORPH=True \
		-DCMAKE_BUILD_TYPE=Debug
	make -j4
	sudo make install && echo "Done: libVNF built and installed." 
	exit 0
fi
