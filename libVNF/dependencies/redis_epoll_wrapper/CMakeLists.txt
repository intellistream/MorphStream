cmake_minimum_required(VERSION 3.5.1)

# Project
project(redis_epoll_wrapper)

# Release type
set(CMAKE_BUILD_TYPE Release)

# Description
set(b "redis_epoll_wrapper is a wrapper on redis server to talk to VNFs built on libvnf")

# Compiler version
set(CMAKE_CXX_STANDARD 11)

# Find libraries
find_library(VNF_LIB vnf-kernel-static)
find_library(EVENT_LIB event)
find_library(BOOST_SYSTEM_LIB boost_system)
find_library(KVS_REDIS_LIB kvs_redis HINT lib)

# Executable
add_executable(redis_epoll_wrapper redis_epoll_wrapper.cpp)

# Link libraries to executable
target_link_libraries(redis_epoll_wrapper PUBLIC ${VNF_LIB} ${EVENT_LIB} ${KVS_REDIS_LIB} ${BOOST_SYSTEM_LIB})
