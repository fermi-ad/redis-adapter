cmake_minimum_required(VERSION 3.14)
project(redis-adapter LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find our cmake modules
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# Create lists of headers and sources with complete path based on our files
file(GLOB REDIS_ADAPTER_SOURCES RedisAdapter.cpp)
file(GLOB REDIS_ADAPTER_HEADERS RedisConnection.hpp RedisAdapter.hpp RedisAdapterTempl.hpp RedisCache.hpp ThreadPool.hpp)

# Create a list of the directories our headers are in
include(GetDirectoriesOfFiles)
get_directories_of_files("${REDIS_ADAPTER_HEADERS}" REDIS_ADAPTER_INCLUDE_DIRS)
list(REMOVE_DUPLICATES REDIS_ADAPTER_INCLUDE_DIRS)
list(APPEND REDIS_ADAPTER_INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/hiredis)
list(APPEND REDIS_ADAPTER_INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/redis-plus-plus/src)
list(APPEND REDIS_ADAPTER_INCLUDE_DIRS ${PROJECT_BINARY_DIR}/redis-plus-plus/src) # for hiredis_features.h

# Link static hiredis and static redis++ to redis-adapter
list(APPEND REDIS_ADAPTER_LIBRARIES hiredis)
list(APPEND REDIS_ADAPTER_LIBRARIES redis++_static)

# Try to get the git commit hash
execute_process(COMMAND git rev-parse HEAD WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  OUTPUT_VARIABLE REDIS_ADAPTER_GIT_COMMIT OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT REDIS_ADAPTER_GIT_COMMIT)
  set(REDIS_ADAPTER_GIT_COMMIT "unknown")
endif()
message(STATUS "REDIS_ADAPTER_GIT_COMMIT ${REDIS_ADAPTER_GIT_COMMIT}")

# Add git commit hash to compiler definitions globally
add_compile_definitions(REDIS_ADAPTER_GIT_COMMIT="${REDIS_ADAPTER_GIT_COMMIT}")

# Send the lists to the project that wants to include them
get_directory_property(HAS_PARENT PARENT_DIRECTORY)
if(HAS_PARENT)
  set(REDIS_ADAPTER_SOURCES ${REDIS_ADAPTER_SOURCES} PARENT_SCOPE)
  set(REDIS_ADAPTER_HEADERS ${REDIS_ADAPTER_HEADERS} PARENT_SCOPE)
  set(REDIS_ADAPTER_LIBRARIES ${REDIS_ADAPTER_LIBRARIES} PARENT_SCOPE)
  set(REDIS_ADAPTER_INCLUDE_DIRS ${REDIS_ADAPTER_INCLUDE_DIRS} PARENT_SCOPE)
  set(REDIS_ADAPTER_COMPILER_FEATURES cxx_std_17 PARENT_SCOPE)
endif()

# Let redis++ see hiredis and redis-adapter see redis++ and hiredis
include_directories(${REDIS_ADAPTER_INCLUDE_DIRS})

# ADD HIREDIS
# Turn off shared libraries so hiredis wont make one
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
# We dont want hiredis tests built
option(DISABLE_TESTS "If tests should be compiled or not" ON)
add_subdirectory(hiredis)
# Make hiredis PIC for redis-adapter shared library
set_target_properties(hiredis PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ADD REDIS++
# We dont want redis++ tests built
option(REDIS_PLUS_PLUS_BUILD_TEST "Build tests for redis++" OFF)
# Turn off redis++ specific shared library generation
option(REDIS_PLUS_PLUS_BUILD_SHARED "Build shared library" OFF)
add_subdirectory(redis-plus-plus)
# Make redis++ PIC for redis-adapter shared library
set_target_properties(redis++_static PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Build redis-adapter-test and googletest if REDIS_ADAPTER_TEST is true
# Specify on command-line: "cmake -D REDIS_ADAPTER_TEST=1 .."
if(REDIS_ADAPTER_TEST)
  message(STATUS "REDIS_ADAPTER_TEST")
  # We dont want googlemock built
  option(BUILD_GMOCK "Builds the googlemock subproject" OFF)
  enable_testing()
  add_subdirectory(googletest)
  include(GoogleTest)
  add_executable(redis-adapter-test ${REDIS_ADAPTER_SOURCES} test.cpp)
  target_link_libraries(redis-adapter-test ${REDIS_ADAPTER_LIBRARIES} GTest::gtest_main)
  gtest_discover_tests(redis-adapter-test)
endif()

# Build redis-adapter-benchmark and googlebenchmark if REDIS_ADAPTER_BENCHMARK is true
# Specify on command-line: "cmake -D REDIS_ADAPTER_BENCHMARK=1 .."
if(REDIS_ADAPTER_BENCHMARK)
  message(STATUS "REDIS_ADAPTER_BENCHMARK")
  # Disable Google Test dependency for benchmark
  set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "Disable benchmark tests based on Google Test")
  set(BENCHMARK_USE_BUNDLED_GTEST OFF CACHE BOOL "Disable bundled Google Test in benchmark")

  # Add Google Benchmark as a submodule
  add_subdirectory(benchmark)

  # Create the benchmark target
  add_executable(redis-adapter-benchmark ${REDIS_ADAPTER_SOURCES} benchmark.cpp)

  # Link the benchmark target with the necessary libraries, including Google Benchmark
  target_link_libraries(redis-adapter-benchmark ${REDIS_ADAPTER_LIBRARIES} benchmark::benchmark)
endif()
