add_executable(test test.cpp)

target_link_libraries(test PRIVATE impl::impl)

install(
  TARGETS test
  RUNTIME DESTINATION bin
)
