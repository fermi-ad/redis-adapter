# Device Digital Twin Adapter
# Generic twin: reads any YAML device config and serves dummy data over Redis

include(FetchContent)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        0.8.0
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

add_executable(device-twin main.cpp)

target_link_libraries(device-twin
  PRIVATE
    redis-adapter-lite
    yaml-cpp::yaml-cpp
)

install(TARGETS device-twin RUNTIME DESTINATION bin)
install(DIRECTORY configs/ DESTINATION etc/adapters/device-twin
  FILES_MATCHING PATTERN "*.yml")
