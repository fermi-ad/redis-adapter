# Demux Adapter
# Splits a single interleaved input into N output channels

include(FetchContent)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        0.8.0
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

add_executable(demux main.cpp)

target_include_directories(demux PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../common)

target_link_libraries(demux
  PRIVATE
    redis-adapter-lite
    yaml-cpp::yaml-cpp
)

install(TARGETS demux RUNTIME DESTINATION bin)
install(DIRECTORY configs/ DESTINATION etc/adapters/demux
  FILES_MATCHING PATTERN "*.yml")
