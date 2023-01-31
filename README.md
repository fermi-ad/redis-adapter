# RedisAdapter
C++ Adapter which wraps redis++ to communicate to the instrumentation redis database

# Build Instructions CMake 
To integrate the RedisAdapter into your CMake project:
1. Add the RedisAdapter repository to your project's directory structure.
2. Include the RedisAdapter project using add_subdirectory(RedisAdapter)
3. Add ```${REDIS_ADAPTER_SOURCES}``` to your list of sources to build. One way to do that is to add ```${REDIS_ADAPTER_SOURCES}``` where you create your executable target: ```add_executable(RedisAdapterTest test.cpp ${REDIS_ADAPTER_SOURCES})```
4. Include the ```${REDIS_ADAPTER_INCLUDE_DIRS}``` in your include directories using ```include_directories(${REDIS_ADAPTER_INCLUDE_DIRS})```
5. Link against ```${REDIS_ADAPTER_LIBRARIES}``` by adding it to your target's libraries list: ```target_link_libraries(RedisAdapterTest ${REDIS_ADAPTER_LIBRARIES})```
6. Require ```${REDIS_ADAPTER_COMPILER_FEATURES}``` by adding it to your target's compiler features list: ```target_compile_features(RedisAdapterTest PUBLIC ${REDIS_ADAPTER_COMPILER_FEATURES})```

Example CMakeLists.txt:
```cmake
cmake_minimum_required(VERSION 3.6)
project(RedisAdapterTest LANGUAGES CXX)

# Inlcude the CMakeLists.txt file for redis addapter
add_subdirectory(RedisAdapter)

# Compile this projects sources and RedisAdapter's sources into a single executable called RedisAdapterTest
add_executable(RedisAdapterTest test.cpp ${REDIS_ADAPTER_SOURCES})
# Add Redis Addapter's inlude directories
include_directories(${REDIS_ADAPTER_INCLUDE_DIRS})
# link Redis addapter's library dependencies agains our executable
target_link_libraries(RedisAdapterTest ${REDIS_ADAPTER_LIBRARIES})

# Require Redis Adapter's compiler features.
target_compile_features(RedisAdapterTest PUBLIC ${REDIS_ADAPTER_COMPILER_FEATURES})
```
