cmake_minimum_required(VERSION 3.10)
project(tidesdb_jni C)

set(CMAKE_C_STANDARD 11)

# Find JNI
find_package(JNI REQUIRED)

# Find TidesDB
find_library(TIDESDB_LIBRARY tidesdb HINTS /usr/local/lib /opt/tidesdb/lib)
find_path(TIDESDB_INCLUDE_DIR tidesdb/db.h HINTS /usr/local/include /opt/tidesdb/include)

if(NOT TIDESDB_LIBRARY)
    message(FATAL_ERROR "TidesDB library not found")
endif()

if(NOT TIDESDB_INCLUDE_DIR)
    message(FATAL_ERROR "TidesDB headers not found")
endif()

# Create shared library
add_library(tidesdb_jni SHARED com_tidesdb_TidesDB.c)

target_include_directories(tidesdb_jni PRIVATE
    ${JNI_INCLUDE_DIRS}
    ${TIDESDB_INCLUDE_DIR}
)

target_link_libraries(tidesdb_jni
    ${TIDESDB_LIBRARY}
    ${JNI_LIBRARIES}
)

# Set output name based on platform
if(APPLE)
    set_target_properties(tidesdb_jni PROPERTIES
        SUFFIX ".dylib"
        PREFIX "lib"
    )
elseif(WIN32)
    set_target_properties(tidesdb_jni PROPERTIES
        SUFFIX ".dll"
        PREFIX ""
    )
else()
    set_target_properties(tidesdb_jni PROPERTIES
        SUFFIX ".so"
        PREFIX "lib"
    )
endif()

# Install
install(TARGETS tidesdb_jni
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)
