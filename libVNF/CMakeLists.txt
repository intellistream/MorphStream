cmake_minimum_required(VERSION 3.5.0)
project(libVNF)

set(PROJECT_DESCRIPTION "libvnf is a library to easily build custom scalable high performance Virtual Network Functions (VNFs)")
# Release type
set(CMAKE_BUILD_TYPE Debug)

# Project version
set(PROJECT_VERSION_MAJOR 0)
set(PROJECT_VERSION_MINOR 9)
set(PROJECT_VERSION_PATCH 1)
set(PROJECT_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}")

# Dependency Paths.
option(MODE "Standalone or Shared lib to build." ON)
option(BACKEND_MORPH "If enable compilation with morphStream." ON)

# Installation Paths.
set(LIBRARY_INSTALL_DIR /usr/lib)
set(LIBRARY_INSTALL_DIR2 /usr/local/lib)
set(HEADERS_INSTALL_DIR /usr/include/libvnf)
set(HEADERS_INSTALL_DIR2 /usr/local/include/libvnf)

set(CMAKE_CXX_STANDARD 11)
if (${CMAKE_BUILD_TYPE} STREQUAL Debug)
	message(STATUS "Building in Debug mode.")
	add_definitions(-DDEBUG=true)
	set(CMAKE_CXX_FLAGS "-Wno-deprecated-declarations -Wno-pointer-arith -lnuma -lrt -DLIBVNF_STACK=1 -g -ljsoncpp -O0")
else ()
	message(STATUS "Building in Release mode.")
	set(CMAKE_CXX_FLAGS "-Wno-deprecated-declarations -Wno-pointer-arith -lnuma -lrt -DLIBVNF_STACK=1 -ljsoncpp -O3")
endif()

# Sources and Headers
file(GLOB_RECURSE HEADERS "include/*hpp")
file(GLOB_RECURSE SOURCES "src/datastore/*cpp" "src/kernel/*cpp")

# Include
include_directories(include)

# Modes to compile libVNF libraries
if (${MODE} STREQUAL STANDALONE)
	message(STATUS "Using ${MODE} for libVNF")

    add_definitions(-DSTANDALONE)

	# Library
	# add_library(vnf-kernel-dynamic SHARED ${SOURCES} ${HEADERS})
	add_library(vnf-kernel-static STATIC ${SOURCES} ${HEADERS})

	# Install
	install(TARGETS vnf-kernel-static DESTINATION ${LIBRARY_INSTALL_DIR})
	install(TARGETS vnf-kernel-static DESTINATION ${LIBRARY_INSTALL_DIR2})
	# install(TARGETS vnf-kernel-dynamic DESTINATION ${LIBRARY_INSTALL_DIR})
	# install(TARGETS vnf-kernel-dynamic DESTINATION ${LIBRARY_INSTALL_DIR2})
	install(DIRECTORY include/ DESTINATION ${HEADERS_INSTALL_DIR})
	install(DIRECTORY include/ DESTINATION ${HEADERS_INSTALL_DIR2})

  	# Installation target 
  	set(STACK_TARGET vnf-kernel-static)
  	# set(STACK_TARGET vnf-kernel-dynamic)
elseif (${MODE} STREQUAL SHARED)
	message(STATUS "Using ${MODE} for libVNF")
    # add_definitions(-DSTANDALONE=true)

	# Library
	add_library(vnf-kernel-dynamic SHARED ${SOURCES} ${HEADERS})
	# add_library(vnf-kernel-static STATIC ${SOURCES} ${HEADERS})

	# Install
	# install(TARGETS vnf-kernel-static DESTINATION ${LIBRARY_INSTALL_DIR})
	# install(TARGETS vnf-kernel-static DESTINATION ${LIBRARY_INSTALL_DIR2})
	install(TARGETS vnf-kernel-dynamic DESTINATION ${LIBRARY_INSTALL_DIR})
	install(TARGETS vnf-kernel-dynamic DESTINATION ${LIBRARY_INSTALL_DIR2})
	install(DIRECTORY include/ DESTINATION ${HEADERS_INSTALL_DIR})
	install(DIRECTORY include/ DESTINATION ${HEADERS_INSTALL_DIR2})

  	# Installation target 
  	# set(STACK_TARGET vnf-kernel-static)
  	set(STACK_TARGET vnf-kernel-dynamic)
endif()

if (${BACKEND_MORPH})
	set(JAVA_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
	# set(JAVA_JNI_INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/utils/java/Loader/Loader.java)
	set(JNI_HEADERS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
  	set(JAVA_INCLUDE_PATH "$ENV{JAVA_HOME}/include")

	message("Compiling with morphStream.")
	add_definitions(-DBACKEND_MORPH)

	# Check environment.
	find_package(Java REQUIRED)
	if(JAVA_FOUND)
		message(STATUS "Java found: ${Java_JAVA_EXECUTABLE}")
		message(STATUS "Java compiler: ${Java_JAVAC_EXECUTABLE}")
		message(STATUS "Java include path: ${JAVA_INCLUDE_PATH}")
	else()
		message(FATAL_ERROR "Java not found. Please install Java and set JAVA_HOME.")
	endif()
	# if(Java_VERSION_STRING VERSION_LESS "1.8")
	# 	message(FATAL_ERROR "Java version 1.8 or higher is required.")
	# endif()
	message(STATUS "Java compiler flags: ${Java_COMPILE_FLAGS}")

	# Check if JNI is supported
	if(JAVA_FOUND AND EXISTS "${JAVA_INCLUDE_PATH}/jni.h")
		include_directories(${JAVA_INCLUDE_PATH})
		include_directories(${JAVA_INCLUDE_PATH}/linux)
	else()
		message(FATAL_ERROR "JNI headers not found. Please make sure Java development tools are installed.")
	endif()

	# Declair dependency for the main library. Choose to build to /lib.
	# add_custom_target(jni_headers_generation ALL
	#	COMMAND ${Java_JAVAC_EXECUTABLE} -cp ${JAVA_SOURCE_DIR} -h ${JNI_HEADERS_DIR} ${JAVA_JNI_INTERFACE}
	#	WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
	# 	COMMENT "Generating JNI headers"
	# )

	# add_dependencies(${STACK_TARGET} jni_headers_generation)

	include_directories(${JNI_HEADERS_DIR})
	message(STATUS "Use JNI header in: ${JNI_HEADERS_DIR}")
endif()
