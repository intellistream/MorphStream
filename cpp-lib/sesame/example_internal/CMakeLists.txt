add_executable(bar bar.cpp)

target_link_libraries(bar PRIVATE join::join)

install(
  TARGETS bar
  RUNTIME DESTINATION bin
)
