rm -dfr build && mkdir build && cd build
CC=/usr/bin/gcc CXX=/usr/bin/g++ cmake -DCMAKE_PREFIX_PATH=$HOME/.local ..  \
	&& make -j8
cd ../java_peer && mvn clean install