# BCM Composite Adapter
# Single-process pipeline: demux → background subtract → gated/ungated integration

include(FetchContent)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        0.8.0
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

add_executable(bcm main.cpp)

target_include_directories(bcm PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../common)

target_link_libraries(bcm
  PRIVATE
    redis-adapter-lite
    yaml-cpp::yaml-cpp
)

install(TARGETS bcm RUNTIME DESTINATION bin)
install(DIRECTORY configs/ DESTINATION etc/adapters/bcm
  FILES_MATCHING PATTERN "*.yml")
