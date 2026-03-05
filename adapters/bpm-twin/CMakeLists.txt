# BPM Digital Twin
# Simulates a 4-button BPM with per-channel IQ signals modulated by beam position

include(FetchContent)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        0.8.0
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

add_executable(bpm-twin main.cpp)

target_link_libraries(bpm-twin
  PRIVATE
    redis-adapter-lite
    yaml-cpp::yaml-cpp
)

install(TARGETS bpm-twin RUNTIME DESTINATION bin)
install(DIRECTORY configs/ DESTINATION etc/adapters/bpm-twin
  FILES_MATCHING PATTERN "*.yml")
