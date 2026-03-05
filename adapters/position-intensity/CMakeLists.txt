# Position-Intensity Adapter
# output[i] = (A[i] - B[i]) / (A[i] + B[i])

include(FetchContent)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        0.8.0
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

add_executable(position-intensity main.cpp)

target_include_directories(position-intensity PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../common)

target_link_libraries(position-intensity
  PRIVATE
    redis-adapter-lite
    yaml-cpp::yaml-cpp
)

install(TARGETS position-intensity RUNTIME DESTINATION bin)
install(DIRECTORY configs/ DESTINATION etc/adapters/position-intensity
  FILES_MATCHING PATTERN "*.yml")
