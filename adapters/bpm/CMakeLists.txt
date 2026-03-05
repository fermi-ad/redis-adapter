# BPM Composite Adapter
# Single-process pipeline: demux → magnitude → position/fft/filter/integrate

include(FetchContent)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        0.8.0
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

add_executable(bpm main.cpp)

target_include_directories(bpm PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../common)

target_link_libraries(bpm
  PRIVATE
    redis-adapter-lite
    yaml-cpp::yaml-cpp
)

install(TARGETS bpm RUNTIME DESTINATION bin)
install(DIRECTORY configs/ DESTINATION etc/adapters/bpm
  FILES_MATCHING PATTERN "*.yml")
