cmake_minimum_required(VERSION 3.14)
project(redis-adapter-lite VERSION 1.0.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Git commit hash
execute_process(COMMAND git rev-parse HEAD
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  OUTPUT_VARIABLE REDIS_ADAPTER_GIT_COMMIT
  OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT REDIS_ADAPTER_GIT_COMMIT)
  set(REDIS_ADAPTER_GIT_COMMIT "unknown")
endif()
message(STATUS "REDIS_ADAPTER_GIT_COMMIT ${REDIS_ADAPTER_GIT_COMMIT}")

# ---- Hiredis submodule ----
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(DISABLE_TESTS "Disable hiredis tests" ON)
add_subdirectory(hiredis)
set_target_properties(hiredis PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ---- RedisAdapterLite library ----
add_library(redis-adapter-lite
  src/RAL_Time.cpp
  src/HiredisReply.cpp
  src/HiredisConnection.cpp
  src/RedisAdapterLite.cpp
)

target_include_directories(redis-adapter-lite
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include/redis-adapter-lite>
)

target_link_libraries(redis-adapter-lite
  PUBLIC hiredis
)

target_compile_definitions(redis-adapter-lite PRIVATE
  REDIS_ADAPTER_GIT_COMMIT=${REDIS_ADAPTER_GIT_COMMIT}
)

set_target_properties(redis-adapter-lite PROPERTIES
  POSITION_INDEPENDENT_CODE ON
)

# Export for parent projects using add_subdirectory
get_directory_property(HAS_PARENT PARENT_DIRECTORY)
if(HAS_PARENT)
  # Target redis-adapter-lite is already available to parent via add_subdirectory
endif()

# ---- Tests ----
option(RAL_BUILD_TESTS "Build RedisAdapterLite tests" OFF)
if(RAL_BUILD_TESTS)
  message(STATUS "RAL_BUILD_TESTS enabled")
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.15.2
  )
  set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  enable_testing()
  include(GoogleTest)

  # Unit tests
  file(GLOB RAL_UNIT_TESTS tests/unit/*.cpp)
  add_executable(ral-test-unit ${RAL_UNIT_TESTS})
  target_link_libraries(ral-test-unit redis-adapter-lite GTest::gtest_main)
  gtest_discover_tests(ral-test-unit TEST_PREFIX "unit/")

  # Integration tests
  file(GLOB RAL_INTEGRATION_TESTS tests/integration/*.cpp)
  add_executable(ral-test-integration ${RAL_INTEGRATION_TESTS})
  target_link_libraries(ral-test-integration redis-adapter-lite GTest::gtest_main)
  gtest_discover_tests(ral-test-integration TEST_PREFIX "integration/")

  # Regression tests
  file(GLOB RAL_REGRESSION_TESTS tests/regressions/*.cpp)
  add_executable(ral-test-regressions ${RAL_REGRESSION_TESTS})
  target_link_libraries(ral-test-regressions redis-adapter-lite GTest::gtest_main)
  gtest_discover_tests(ral-test-regressions TEST_PREFIX "regressions/")
endif()

# ---- Benchmarks ----
option(RAL_BUILD_BENCHMARKS "Build RedisAdapterLite benchmarks" OFF)
if(RAL_BUILD_BENCHMARKS)
  message(STATUS "RAL_BUILD_BENCHMARKS enabled")
  include(FetchContent)
  FetchContent_Declare(
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG        v1.9.1
  )
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(benchmark)

  add_executable(ral-benchmark test/benchmark_lite.cpp)
  target_link_libraries(ral-benchmark redis-adapter-lite benchmark::benchmark)
endif()

# ---- Install ----
install(TARGETS redis-adapter-lite
  EXPORT redis-adapter-lite-targets
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)

install(DIRECTORY src/
  DESTINATION include/redis-adapter-lite
  FILES_MATCHING PATTERN "*.hpp"
)

install(EXPORT redis-adapter-lite-targets
  FILE redis-adapter-lite-config.cmake
  DESTINATION lib/cmake/redis-adapter-lite
)
