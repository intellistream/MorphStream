cmake_minimum_required(VERSION 3.9)
project(streamjoin)
set(CMAKE_CXX_STANDARD 14)


# Set SOURCES variable
set(SOURCES
        src/streamjoin.cpp
        )

# Set HEADERS_PUBLIC variable (public headers, included in the library)
set(HEADERS_PUBLIC
        include/streamjoin.h
        include/types.h
        )

# Set HEADERS_PRIVATE variable
# (these private headers won't be included in the library)
# set(HEADERS_PRIVATE
#     foo_private.h
# )

include(${PROJECT_SOURCE_DIR}/cmake/LibraryConfig.cmake)