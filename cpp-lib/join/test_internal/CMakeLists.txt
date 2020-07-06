add_executable(test main.cpp)

target_link_libraries(test PRIVATE impl::impl)

install(
  TARGETS test
  RUNTIME DESTINATION bin
)
