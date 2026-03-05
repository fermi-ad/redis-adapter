# Filter Adapter
# Exponential moving average: state[i] = alpha*A[i] + (1-alpha)*state[i]

include(FetchContent)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        0.8.0
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

add_executable(filter main.cpp)

target_include_directories(filter PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../common)

target_link_libraries(filter
  PRIVATE
    redis-adapter-lite
    yaml-cpp::yaml-cpp
)

install(TARGETS filter RUNTIME DESTINATION bin)
install(DIRECTORY configs/ DESTINATION etc/adapters/filter
  FILES_MATCHING PATTERN "*.yml")
