cmake_minimum_required(VERSION 3.9)
project(streamjoin)
set(CMAKE_CXX_STANDARD 14)

#add_executable(streamjoin main.cpp)

add_library(streamjoin SHARED main.cpp)

#install the library to /usr/local/lib
#/usr/local is the default path.
install (TARGETS streamjoin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin)