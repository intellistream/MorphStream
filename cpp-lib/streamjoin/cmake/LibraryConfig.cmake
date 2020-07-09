# Library name (by default is the project name)
if(NOT LIBRARY_NAME)
    set(LIBRARY_NAME streamjoin)
endif()

add_library(${LIBRARY_NAME}
        SHARED
        ${SOURCES}
        ${HEADERS_PUBLIC}
        ${HEADERS_PRIVATE}
        )

# Global includes. Used by all targets
# Note:
#   - header can be included by C++ code `#include <foo/foo.h>`
#   - header location in project: ${CMAKE_CURRENT_BINARY_DIR}/generated_headers
target_include_directories(
        ${LIBRARY_NAME} PUBLIC
        "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
        "$<BUILD_INTERFACE:${GENERATED_HEADERS_DIR}>"
        "$<INSTALL_INTERFACE:.>"
)

# Layout. This works for all platforms:
#   - $HOME/<PROJECT-NAME>/lib/
#   - $HOME/<PROJECT-NAME>/include/
set(CMAKE_INSTALL_LIBDIR "$ENV{HOME}/${PROJECT_NAME}/lib")
set(CMAKE_INSTALL_BINDIR " $ENV{HOME}/${PROJECT_NAME}/bin")
set(CMAKE_INSTALL_INCLUDEDIR "$ENV{HOME}/${PROJECT_NAME}/include")

# Configuration
set(GENERATED_DIR       "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(VERSION_CONFIG_FILE "${GENERATED_DIR}/${PROJECT_NAME}ConfigVersion.cmake")
set(PROJECT_CONFIG_FILE "${GENERATED_DIR}/${PROJECT_NAME}Config.cmake")
set(TARGETS_EXPORT_NAME "${PROJECT_NAME}Targets")

# Targets:
#   - <prefix>/lib/libfoo.a
#   - header location after install: <prefix>/foo/foo.h
#   - headers can be included by C++ code `#include <foo/foo.h>`
install(
        TARGETS              "${LIBRARY_NAME}"
        EXPORT               "${TARGETS_EXPORT_NAME}"
        LIBRARY DESTINATION  "${CMAKE_INSTALL_LIBDIR}"
        ARCHIVE DESTINATION  "${CMAKE_INSTALL_LIBDIR}"
        RUNTIME DESTINATION  "${CMAKE_INSTALL_BINDIR}"
        INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

# Headers:
#   - foo/*.h -> $HOME/include/foo/*.h
install(
        FILES ${HEADERS_PUBLIC}
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${LIBRARY_FOLDER}"
)