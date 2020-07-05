add_executable(bar bar.cpp)

target_link_libraries(bar PRIVATE sesame::sesame)

install(
  TARGETS bar
  RUNTIME DESTINATION bin
)
