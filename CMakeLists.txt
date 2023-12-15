cmake_minimum_required(VERSION 3.14)
project(RedisAdapter LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# todo put this somewhere where we can reuse it more broadly
# function to extract include directories from a list of files
# takes two arguments:
#   - _files: list of files
#   - _var: variable to store the list of include directories
#   - example usage get_directory_of_files("${REDIS_ADAPTER_HEADERS}" REDIS_ADAPTER_INCLUDE_DIRS)
#         where REDIS_ADAPTER_HEADERS contains the names of the headers used and
#         REDIS_ADAPTER_INCLUDE_DIRS is the name of the variable you want the result stored in
function(get_directories_of_files _files _var)
    # iterate over the list of files
    foreach(_file ${_files})
        # extract the directory of each header
        get_filename_component(_dir ${_file} DIRECTORY)
        # add the directory to the list of include directories
        list(APPEND directories ${_dir})
    endforeach()
    # remove duplicates from the list of include directories
    list(REMOVE_DUPLICATES directories)
    # set the variable to the parent scope so it can be used outside the function
    set(${_var} ${directories} PARENT_SCOPE)
endfunction()

# Create a lists of headers and sources with complete path based on our source files
file(GLOB REDIS_ADAPTER_SOURCES RedisAdapter.cpp)
file(GLOB REDIS_ADAPTER_HEADERS RedisConnection.h RedisAdapter.hpp)

# Create a list of the directories our headers are in
get_directories_of_files("${REDIS_ADAPTER_HEADERS}" REDIS_ADAPTER_INCLUDE_DIRS)
list(REMOVE_DUPLICATES REDIS_ADAPTER_INCLUDE_DIRS)
list(APPEND REDIS_ADAPTER_INCLUDE_DIRS hiredis)
list(APPEND REDIS_ADAPTER_INCLUDE_DIRS redis-plus-plus/src)

list(APPEND REDIS_ADAPTER_LIBRARIES hiredis)
list(APPEND REDIS_ADAPTER_LIBRARIES redis++_static)

# Send the lists to the project that wants to include them.
set(REDIS_ADAPTER_SOURCES ${REDIS_ADAPTER_SOURCES} PARENT_SCOPE)
set(REDIS_ADAPTER_HEADERS ${REDIS_ADAPTER_HEADERS} PARENT_SCOPE)
set(REDIS_ADAPTER_LIBRARIES ${REDIS_ADAPTER_LIBRARIES} PARENT_SCOPE)
set(REDIS_ADAPTER_INCLUDE_DIRS ${REDIS_ADAPTER_INCLUDE_DIRS} PARENT_SCOPE)
set(REDIS_ADAPTER_COMPILER_FEATURES cxx_std_17 PARENT_SCOPE)

option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(DISABLE_TESTS "If tests should be compiled or not" ON)
add_subdirectory(hiredis)

option(REDIS_PLUS_PLUS_BUILD_TEST "Build tests for redis++" OFF)
option(REDIS_PLUS_PLUS_BUILD_SHARED "Build shared library" OFF)
add_subdirectory(redis-plus-plus)

add_library(RedisAdapter_static STATIC ${REDIS_ADAPTER_SOURCES})
add_library(RedisAdapter SHARED ${REDIS_ADAPTER_SOURCES})

option(BUILD_GMOCK "Builds the googlemock subproject" OFF)
enable_testing()
add_subdirectory(googletest)
include(GoogleTest)
include_directories(${REDIS_ADAPTER_INCLUDE_DIRS})
add_executable(TestRedisAdapter ${REDIS_ADAPTER_SOURCES} test.cpp)
target_link_libraries(TestRedisAdapter ${REDIS_ADAPTER_LIBRARIES} GTest::gtest_main)
target_compile_features(TestRedisAdapter PUBLIC ${REDIS_ADAPTER_COMPILER_FEATURES})
gtest_discover_tests(TestRedisAdapter)
