cmake_minimum_required(VERSION 3.9)
project(App)

# find package Foo (example library)
find_package(sesame 1.0.0 CONFIG REQUIRED)

# add binary
add_executable(app external_app.cpp)

# add library
target_link_libraries(app PRIVATE join::join)
