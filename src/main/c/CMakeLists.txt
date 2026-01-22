cmake_minimum_required(VERSION 3.10)
project(tidesdb_jni C)

set(CMAKE_C_STANDARD 11)

# Find JNI
find_package(JNI REQUIRED)

# Find TidesDB
find_library(TIDESDB_LIBRARY tidesdb HINTS /usr/local/lib /opt/tidesdb/lib ${CMAKE_PREFIX_PATH}/lib)
find_path(TIDESDB_INCLUDE_DIR tidesdb/db.h HINTS /usr/local/include /opt/tidesdb/include ${CMAKE_PREFIX_PATH}/include)

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

# On Windows with static tidesdb, we need to link compression libraries explicitly
# using the same approach as tidesdb's CMakeLists.txt
if(WIN32)
    find_package(zstd CONFIG REQUIRED)
    find_package(Snappy CONFIG REQUIRED)
    find_package(lz4 CONFIG REQUIRED)
    find_package(PThreads4W REQUIRED)
    
    target_link_libraries(tidesdb_jni
        ${TIDESDB_LIBRARY}
        $<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>
        Snappy::snappy
        lz4::lz4
        PThreads4W::PThreads4W
        stdc++
        ${JNI_LIBRARIES}
    )
else()
    target_link_libraries(tidesdb_jni
        ${TIDESDB_LIBRARY}
        ${JNI_LIBRARIES}
    )
endif()

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
# On macOS, shared libraries (.dylib) are treated as LIBRARY
# On Windows, DLLs are treated as RUNTIME
# On Linux, shared libraries (.so) are treated as LIBRARY
install(TARGETS tidesdb_jni
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION lib
    ARCHIVE DESTINATION lib
)
