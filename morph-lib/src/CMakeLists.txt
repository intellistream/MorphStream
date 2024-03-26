list(APPEND source_ddckv 
    nm.cc 
    ib.cc 
    server_mm.cc 
    server.cc
    kv_utils.cc
    hashtable.cc
    client_mm.cc
    client.cc
    client_cr.cc)

add_library(libddckv STATIC ${source_ddckv})

target_compile_options(
    libddckv
    PRIVATE
    "-O2"
    # ${CMAKE_CXX_FLAGS_DEBUG}
    "-g"
    # "-D_DEBUG"
)

target_link_libraries(libddckv ${Boost_LIBRARIES} boost_context boost_fiber tbb)

