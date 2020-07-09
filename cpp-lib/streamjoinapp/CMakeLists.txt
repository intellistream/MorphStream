cmake_minimum_required(VERSION 3.16)
project(streamjoinapp)
set(CMAKE_CXX_STANDARD 14)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

add_executable(streamjoinapp main.cpp)

include(FindPkgConfig)
find_package(StreamJoin REQUIRED)
include_directories(${STREAMJOIN_INCLUDE_DIR})
target_link_libraries(${PROJECT_NAME} ${STREAMJOIN_LIBRARY})
