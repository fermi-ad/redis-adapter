# BLM Composite Adapter
# Single-process pipeline: demux → raw waveforms → integration (10ms/50ms) → averages

include(FetchContent)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        0.8.0
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

add_executable(blm main.cpp)

target_include_directories(blm PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../common)

target_link_libraries(blm
  PRIVATE
    redis-adapter-lite
    yaml-cpp::yaml-cpp
)

install(TARGETS blm RUNTIME DESTINATION bin)
install(DIRECTORY configs/ DESTINATION etc/adapters/blm
  FILES_MATCHING PATTERN "*.yml")
