add_executable(test test.cpp)

target_link_libraries(test PRIVATE join::join)

install(
  TARGETS test
  RUNTIME DESTINATION bin
)
