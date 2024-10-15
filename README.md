# RedisAdapter
C++ adapter which wraps redis++ to communicate to the instrumentation Redis database

# Build Instructions CMake
To build RedisAdapter as a standalone library, including tests and benchmarking (cmake required)
1. From the root directory:

    `cmake -S . -B build -D REDIS_ADAPTER_TEST=1 -D REDIS_ADAPTER_BENCHMARK=1 && cmake --build build`

2. Start Redis using the redis-start.sh script

    `./redis-start.sh`

3. Run the test executable in the build directory

    `./build/redis-adapter-test`

4. Run the benchmark executable in the build directory.

    `./build/redis-adapter-test [host]`

To integrate RedisAdapter into your CMake project:
1. Add the RedisAdapter repository to your project's directory structure.
2. Include the RedisAdapter project using:

    `add_subdirectory(RedisAdapter)`

3. Add `${REDIS_ADAPTER_SOURCES}` to your list of sources to build. One way to do that is to add `${REDIS_ADAPTER_SOURCES}` where you create your executable target:

    `add_executable(RedisAdapterTest main.cpp ${REDIS_ADAPTER_SOURCES})`

4. Include the `${REDIS_ADAPTER_INCLUDE_DIRS}` in your include directories using:

    `include_directories(${REDIS_ADAPTER_INCLUDE_DIRS})`

5. Link against `${REDIS_ADAPTER_LIBRARIES}` by adding it to your target's libraries list:

    `target_link_libraries(RedisAdapterTest ${REDIS_ADAPTER_LIBRARIES})`

6. Require `${REDIS_ADAPTER_COMPILER_FEATURES}` by adding it to your target's compiler features list:

    `target_compile_features(RedisAdapterTest PUBLIC ${REDIS_ADAPTER_COMPILER_FEATURES})`

Example CMakeLists.txt:
```cmake
cmake_minimum_required(VERSION 3.6)
project(RedisAdapterTest LANGUAGES CXX)

# Include the CMakeLists.txt file for RedisAdapter
add_subdirectory(RedisAdapter)

# Compile this project's sources and RedisAdapter's sources into RedisAdapterTest
add_executable(RedisAdapterTest main.cpp ${REDIS_ADAPTER_SOURCES})

# Add RedisAdapter's include directories
include_directories(${REDIS_ADAPTER_INCLUDE_DIRS})

# Link RedisAdapter's library dependencies against our executable
target_link_libraries(RedisAdapterTest ${REDIS_ADAPTER_LIBRARIES})

# Require RedisAdapter's compiler features
target_compile_features(RedisAdapterTest PUBLIC ${REDIS_ADAPTER_COMPILER_FEATURES})
```
