set(FIND_STREAMJOIN_PATHS
        $ENV{HOME}/streamjoin)

find_path(STREAMJOIN_INCLUDE_DIR streamjoin.h
        PATH_SUFFIXES include
        PATHS ${FIND_STREAMJOIN_PATHS}
        )

find_library(STREAMJOIN_LIBRARY
        NAMES streamjoin
        PATH_SUFFIXES lib
        PATHS ${FIND_STREAMJOIN_PATHS})