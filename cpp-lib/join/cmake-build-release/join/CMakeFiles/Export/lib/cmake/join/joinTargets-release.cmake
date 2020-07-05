#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "join::join" for configuration "Release"
set_property(TARGET join::join APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(join::join PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libjoin.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS join::join )
list(APPEND _IMPORT_CHECK_FILES_FOR_join::join "${_IMPORT_PREFIX}/lib/libjoin.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
