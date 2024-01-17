cmake_minimum_required(VERSION 3.14)
project(RedisAdapter LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# "make install" to project directory
set(CMAKE_INSTALL_PREFIX ${PROJECT_SOURCE_DIR}/install)

# Find our cmake modules
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# Create lists of headers and sources with complete path based on our files
file(GLOB REDIS_ADAPTER_SOURCES RedisAdapter.cpp)
file(GLOB REDIS_ADAPTER_HEADERS RedisConnection.hpp RedisAdapter.hpp RedisAdapterTempl.hpp)

# Create a list of the directories our headers are in
include(GetDirectoriesOfFiles)
get_directories_of_files("${REDIS_ADAPTER_HEADERS}" REDIS_ADAPTER_INCLUDE_DIRS)
list(REMOVE_DUPLICATES REDIS_ADAPTER_INCLUDE_DIRS)
list(APPEND REDIS_ADAPTER_INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/hiredis)
list(APPEND REDIS_ADAPTER_INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/redis-plus-plus/src)

# Link static hiredis and static redis++ to RedisAdapter
list(APPEND REDIS_ADAPTER_LIBRARIES hiredis)
list(APPEND REDIS_ADAPTER_LIBRARIES redis++_static)

# Send the lists to the project that wants to include them
set(REDIS_ADAPTER_SOURCES ${REDIS_ADAPTER_SOURCES} PARENT_SCOPE)
set(REDIS_ADAPTER_HEADERS ${REDIS_ADAPTER_HEADERS} PARENT_SCOPE)
set(REDIS_ADAPTER_LIBRARIES ${REDIS_ADAPTER_LIBRARIES} PARENT_SCOPE)
set(REDIS_ADAPTER_INCLUDE_DIRS ${REDIS_ADAPTER_INCLUDE_DIRS} PARENT_SCOPE)
set(REDIS_ADAPTER_COMPILER_FEATURES cxx_std_17 PARENT_SCOPE)

# Let redis++ see hiredis and RedisAdapter see redis++ and hiredis
include_directories(${REDIS_ADAPTER_INCLUDE_DIRS})

# ADD HIREDIS
# Turn off shared libraries so hiredis wont make one
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
# We dont want hiredis tests built
option(DISABLE_TESTS "If tests should be compiled or not" ON)
add_subdirectory(hiredis)
# Make hiredis PIC for RedisAdapter shared library
set_target_properties(hiredis PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ADD REDIS++
# We dont want redis++ tests built
option(REDIS_PLUS_PLUS_BUILD_TEST "Build tests for redis++" OFF)
# Turn off redis++ specific shared library generation
option(REDIS_PLUS_PLUS_BUILD_SHARED "Build shared library" OFF)
add_subdirectory(redis-plus-plus)

# Build RedisAdapter static library
add_library(RedisAdapter_static STATIC ${REDIS_ADAPTER_SOURCES})
target_link_libraries(RedisAdapter_static ${REDIS_ADAPTER_LIBRARIES})

# Build RedisAdapter shared library
add_library(RedisAdapter SHARED ${REDIS_ADAPTER_SOURCES})
target_link_libraries(RedisAdapter ${REDIS_ADAPTER_LIBRARIES})

# "make install" to project directory
install(TARGETS RedisAdapter_static RedisAdapter ARCHIVE DESTINATION lib64)

# Build TestRedisAdapter app with googletest
# We dont want googlemock built
option(BUILD_GMOCK "Builds the googlemock subproject" OFF)
enable_testing()
add_subdirectory(googletest)
include(GoogleTest)
add_executable(TestRedisAdapter ${REDIS_ADAPTER_SOURCES} test.cpp)
target_link_libraries(TestRedisAdapter ${REDIS_ADAPTER_LIBRARIES} GTest::gtest_main)
gtest_discover_tests(TestRedisAdapter)
