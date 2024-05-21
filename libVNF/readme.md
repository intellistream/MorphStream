An VNF runtime modified from libVNF exposing its interface with grpc.

# Dependency 

```bash
sudo apt-get install -y build-essential cmake jsoncpp-dev autoconf libtool pkg-config
```

Clone repo and related thirdparty tools, build tools.
```bash
git clone https://Kailian-Jacy/VNFRuntime/ --recursive --update
cd spdlog && mkdir build && cd build 
```

gRPC and Google Protobuf: (Follow instructions from *https://grpc.io/docs/languages/cpp/quickstart/*)
```bash
export MY_INSTALL_DIR=$HOME/.local
mkdir -p $MY_INSTALL_DIR
export PATH="$MY_INSTALL_DIR/bin:$PATH"
git clone --recurse-submodules -b v1.62.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc
cd grpc
mkdir -p cmake/build
pushd cmake/build
cmake -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
      ../..
make -j 4
make install
popd
```

# Build

```bash
./build.sh
```