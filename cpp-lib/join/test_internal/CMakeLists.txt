add_executable(test main.cpp)

# find package Foo (example library)
find_package(join_impl 1.0.0 CONFIG REQUIRED)

target_link_libraries(test PRIVATE join_impl::join_impl)

install(
  TARGETS test
  RUNTIME DESTINATION bin
)
