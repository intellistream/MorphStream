find_package(GTest REQUIRED)

include_directories(${GTEST_INCLUDE_DIRS})

add_executable(test_nm test_nm.cc ddckv_test.cc)
add_executable(test_kv_utils test_kv_utils.cc ddckv_test.cc)
# add_executable(test_server test_server.cc ddckv_test.cc)
add_executable(test_client_server test_client_server.cc)
add_executable(test_client_client test_client_client.cc)
add_executable(test_remote_nm test_remote_nm.cc ddckv_test.cc)
add_executable(client_kv_shell client_kv_shell.cc)
add_executable(test_mm test_mm.cc ddckv_test.cc)

target_link_libraries(test_remote_nm
    ${GTEST_BOTH_LIBRARIES}
    libddckv
    pthread
    ibverbs
)

target_link_libraries(test_kv_utils
    ${GTEST_BOTH_LIBRARIES}
    libddckv
    pthread
)

target_link_libraries(test_nm
    ${GTEST_BOTH_LIBRARIES}
    libddckv
    pthread
    ibverbs
)

# target_link_libraries(test_server
#     ${GTEST_BOTH_LIBRARIES}
#     libddckv
#     pthread
#     ibverbs
# )

target_link_libraries(test_client_server 
    libddckv
    pthread
    ibverbs
)

target_compile_options(
    test_client_client
    PRIVATE
    # ${CMAKE_CXX_FLAGS_DEBUG}
    # "-g"
    # "-D_DEBUG"
)

target_link_libraries(test_client_client
    ${GTEST_BOTH_LIBRARIES}
    libddckv
    pthread
    ibverbs
)

target_link_libraries(client_kv_shell
    libddckv
    pthread
    ibverbs
)

target_link_libraries(test_mm
    ${GTEST_BOTH_LIBRARIES}
    libddckv
    pthread
    ibverbs
)

gtest_discover_tests(
    test_nm
    test_remote_nm
    test_kv_utils
    # test_server
    test_client_client
    test_mm
)