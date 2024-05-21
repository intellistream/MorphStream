cmake_minimum_required(VERSION 3.8)

project(VNFRuntime C CXX)

# Enable debugging
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")  # Add debugging information
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0") # Disable optimizations

include(./common.cmake)

# Proto file
get_filename_component(proto "${CMAKE_CURRENT_SOURCE_DIR}/message.proto" ABSOLUTE)
get_filename_component(proto_path "${proto}" PATH)

# Other source file
file(GLOB_RECURSE src_files 
	"${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp" 
	"${CMAKE_CURRENT_SOURCE_DIR}/src/vnf/*.cpp"
)

# Generated sources
set(proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/message.pb.cc")
set(proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/message.pb.h")
add_custom_command(
      OUTPUT "${proto_srcs}" "${proto_hdrs}" "${proto_grpc_srcs}" "${proto_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${proto_path}"
        "${proto}"
      DEPENDS "${proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories(${CMAKE_PREFIX_PATH}/include)
include_directories(/usr/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/spdlog/include)

# grpc_proto
add_library(proto
  ${proto_srcs}
  ${proto_hdrs})
target_link_libraries(proto
  ${_PROTOBUF_LIBPROTOBUF})

# Targets greeter_[async_](client|server)
add_executable(VNFRuntime ${src_files})
target_link_libraries(VNFRuntime
proto
absl::flags
absl::flags_parse
jsoncpp
${_REFLECTION}
${_PROTOBUF_LIBPROTOBUF})