cmake_minimum_required(VERSION 3.6)
project(RedisAdapter LANGUAGES CXX)

# <------------ add hiredis dependency --------------->
find_path(HIREDIS_HEADER hiredis)
find_library(HIREDIS_LIB hiredis)

# <------------ add redis-plus-plus dependency -------------->
# NOTE: this should be *sw* NOT *redis++*
find_path(REDIS_PLUS_PLUS_HEADER sw)
find_library(REDIS_PLUS_PLUS_LIB redis++)

# <------------ add pthread dependency -------------->
find_library(PTHREAD_LIB pthread)

file(GLOB REDIS_ADAPTER_SOURCES RedisAdapterCluster.cpp RedisAdapter.cpp RedisAdapterSingle.cpp)
file(GLOB REDIS_ADAPTER_HEADERS IRedisAdapter.hpp RedisAdapterCluster.hpp RedisAdapter.hpp RedisAdapterSingle.hpp ${REDIS_PLUS_PLUS_HEADER} ${HIREDS_HEADER})

set(REDIS_ADAPTER_SOURCES ${REDIS_ADAPTER_SOURCES} PARENT_SCOPE)
set(REDIS_ADAPTER_HEADERS ${REDIS_ADAPTER_HEADERS} PARENT_SCOPE)
set(REDIS_ADAPTER_LIBRARIES ${REDIS_PLUS_PLUS_LIB} ${HIREDIS_LIB} ${PTHREAD_LIB} PARENT_SCOPE)

foreach(_header ${REDIS_ADAPTER_HEADERS})
    get_filename_component(_dir ${_header} DIRECTORY)
    list(APPEND REDIS_ADAPTER_INCLUDE_DIRS ${_dir})
endforeach()
list(REMOVE_DUPLICATES REDIS_ADAPTER_INCLUDE_DIRS)
set(REDIS_ADAPTER_INCLUDE_DIRS ${REDIS_ADAPTER_INCLUDE_DIRS} PARENT_SCOPE)

set(REDIS_ADAPTER_COMPILER_FEATURES cxx_std_17 PARENT_SCOPE)
