# BCM Digital Twin
# Simulates a 4-channel beam current monitor digitizer at 5 MHz

include(FetchContent)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        0.8.0
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

add_executable(bcm-twin main.cpp)

target_link_libraries(bcm-twin
  PRIVATE
    redis-adapter-lite
    yaml-cpp::yaml-cpp
)

install(TARGETS bcm-twin RUNTIME DESTINATION bin)
install(DIRECTORY configs/ DESTINATION etc/adapters/bcm-twin
  FILES_MATCHING PATTERN "*.yml")
