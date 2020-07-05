cmake_minimum_required(VERSION 3.9)

add_executable(bar bar.cpp)

# find package Foo (example library)
find_package(Foo 1.2.3 CONFIG REQUIRED)

target_link_libraries(bar PRIVATE Foo::foo)

install(
  TARGETS bar
  RUNTIME DESTINATION bin
)
