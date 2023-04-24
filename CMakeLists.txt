cmake_minimum_required(VERSION 3.6)
project(RedisAdapter LANGUAGES CXX)

# todo put this somewhere where we can reuse it more broadly
# function to extract include directories from a list of files
# takes two arguments:
#   - _files: list of files
#   - _var: variable to store the list of include directories
#   - example usage get_directory_of_files("${REDIS_ADAPTER_HEADERS}" REDIS_ADAPTER_INCLUDE_DIRS)
#         where REDIS_ADAPTER_HEADERS contains the names of the headers used and REDIS_ADAPTER_INCLUDE_DIRS is the name of the variable you want the result stored in
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

# <------------ add hiredis dependency --------------->
find_path(HIREDIS_HEADER_PATH hiredis)
find_library(HIREDIS_LIB hiredis)
if (NOT HIREDIS_HEADER_PATH OR NOT HIREDIS_LIB)
	message(SEND_ERROR "Did not find hiredis library. Install hiredis library from your package manager")
endif()

# <------------ add redis-plus-plus dependency -------------->
# NOTE: this should be *sw* NOT *redis++*
find_path(REDIS_PLUS_PLUS_HEADER_PATH sw)
find_library(REDIS_PLUS_PLUS_LIB redis++)
if (NOT REDIS_PLUS_PLUS_HEADER_PATH OR NOT REDIS_PLUS_PLUS_LIB)
	message(SEND_ERROR "Did not find redis++ library. Install redis plus plus from here: https://github.com/sewenew/redis-plus-plus")
endif()

# <------------ add pthread dependency -------------->
find_library(PTHREAD_LIB pthread)
if (NOT PTHREAD_LIB)
	message(SEND_ERROR "Did not find pthread library. Install pthread library from your package manager")
endif()

# <------------ add TRACE dependency -------------->
find_path(TRACE_HEADER_PATH TRACE)
if (NOT TRACE_HEADER_PATH)
	message(SEND_ERROR "Did not find TRACE library. Install TRACE library from here: git clone http://cdcvs.fnal.gov/projects/trace-git trace")
endif()

# Create a lists of headers and sources with complete path based on our source files
file(GLOB REDIS_ADAPTER_SOURCES RedisAdapter.cpp)
file(GLOB REDIS_ADAPTER_HEADERS IRedisAdapter.hpp RedisAdapter.hpp)

# Create a list of the directories our headers are in
get_directories_of_files("${REDIS_ADAPTER_HEADERS}" REDIS_ADAPTER_INCLUDE_DIRS)
list(APPEND REDIS_ADAPTER_INCLUDE_DIRS ${HIREDIS_HEADER_PATH})
list(APPEND REDIS_ADAPTER_INCLUDE_DIRS ${REDIS_PLUS_PLUS_HEADER_PATH})
list(APPEND REDIS_ADAPTER_INCLUDE_DIRS ${TRACE_HEADER_PATH})
list(REMOVE_DUPLICATES REDIS_ADAPTER_INCLUDE_DIRS)

# Send the lists to the project that wants to include them.
set(REDIS_ADAPTER_SOURCES ${REDIS_ADAPTER_SOURCES} PARENT_SCOPE)
set(REDIS_ADAPTER_HEADERS ${REDIS_ADAPTER_HEADERS} PARENT_SCOPE)
set(REDIS_ADAPTER_LIBRARIES ${REDIS_PLUS_PLUS_LIB} ${HIREDIS_LIB} ${PTHREAD_LIB} PARENT_SCOPE)
set(REDIS_ADAPTER_INCLUDE_DIRS ${REDIS_ADAPTER_INCLUDE_DIRS} PARENT_SCOPE)
set(REDIS_ADAPTER_COMPILER_FEATURES cxx_std_20 PARENT_SCOPE)
