cmake_minimum_required(VERSION 3.14)
project(RedisAdapter LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find our cmake modules
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# Create lists of headers and sources with complete path based on our files
file(GLOB REDIS_ADAPTER_SOURCES RedisAdapter.cpp)
file(GLOB REDIS_ADAPTER_HEADERS RedisConnection.hpp RedisAdapter.hpp RedisAdapterTempl.hpp RedisCache.hpp)

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
get_directory_property(HAS_PARENT PARENT_DIRECTORY)
if(HAS_PARENT)
  set(REDIS_ADAPTER_SOURCES ${REDIS_ADAPTER_SOURCES} PARENT_SCOPE)
  set(REDIS_ADAPTER_HEADERS ${REDIS_ADAPTER_HEADERS} PARENT_SCOPE)
  set(REDIS_ADAPTER_LIBRARIES ${REDIS_ADAPTER_LIBRARIES} PARENT_SCOPE)
  set(REDIS_ADAPTER_INCLUDE_DIRS ${REDIS_ADAPTER_INCLUDE_DIRS} PARENT_SCOPE)
  set(REDIS_ADAPTER_COMPILER_FEATURES cxx_std_17 PARENT_SCOPE)
endif()

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
# Make redis++ PIC for RedisAdapter shared library
set_target_properties(redis++_static PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Build RedisAdapter shared library
add_library(RedisAdapter_shared SHARED ${REDIS_ADAPTER_SOURCES})
target_link_libraries(RedisAdapter_shared ${REDIS_ADAPTER_LIBRARIES})
set_target_properties(RedisAdapter_shared PROPERTIES LIBRARY_OUTPUT_NAME RedisAdapter)

# Build RedisAdapter static library
add_library(RedisAdapter_static STATIC ${REDIS_ADAPTER_SOURCES})
target_link_libraries(RedisAdapter_static ${REDIS_ADAPTER_LIBRARIES})
set_target_properties(RedisAdapter_static PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(RedisAdapter_static PROPERTIES ARCHIVE_OUTPUT_NAME RedisAdapter)

# "make install" to project directory
set(CMAKE_INSTALL_PREFIX ${PROJECT_SOURCE_DIR}/install)

# These 3 lines must occur here and in this order to quiet warnings and install the headers
install(TARGETS RedisAdapter_shared LIBRARY DESTINATION lib)
set_target_properties(RedisAdapter_shared PROPERTIES PUBLIC_HEADER "${REDIS_ADAPTER_HEADERS}")
install(TARGETS RedisAdapter_shared PUBLIC_HEADER)

# These 3 lines must occur here and in this order to quiet warnings and install the headers
install(TARGETS RedisAdapter_static ARCHIVE DESTINATION lib)
set_target_properties(RedisAdapter_static PROPERTIES PUBLIC_HEADER "${REDIS_ADAPTER_HEADERS}")
install(TARGETS RedisAdapter_static PUBLIC_HEADER)

# Install supporting libs (hiredis, redis++)
install(TARGETS ${REDIS_ADAPTER_LIBRARIES} ARCHIVE DESTINATION lib)

# Build RedisAdapterTest and googletest if REDIS_ADAPTER_TEST is true
# Specify on command-line: "cmake -D REDIS_ADAPTER_TEST=1 .."
if(REDIS_ADAPTER_TEST)
  # We dont want googlemock built
  option(BUILD_GMOCK "Builds the googlemock subproject" OFF)
  enable_testing()
  add_subdirectory(googletest)
  include(GoogleTest)
  add_executable(RedisAdapterTest ${REDIS_ADAPTER_SOURCES} test.cpp)
  target_link_libraries(RedisAdapterTest ${REDIS_ADAPTER_LIBRARIES} GTest::gtest_main)
  gtest_discover_tests(RedisAdapterTest)
endif()
