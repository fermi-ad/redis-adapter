# BLM Digital Twin
# Simulates an 8-channel beam loss monitor digitizer at 1 MHz sample rate

include(FetchContent)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        0.8.0
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

add_executable(blm-twin main.cpp)

target_link_libraries(blm-twin
  PRIVATE
    redis-adapter-lite
    yaml-cpp::yaml-cpp
)

install(TARGETS blm-twin RUNTIME DESTINATION bin)
install(DIRECTORY configs/ DESTINATION etc/adapters/blm-twin
  FILES_MATCHING PATTERN "*.yml")
