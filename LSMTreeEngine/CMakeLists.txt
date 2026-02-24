cmake_minimum_required(VERSION 3.20)

project(LSMTreeEngine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Recursively collect source files
file(GLOB_RECURSE ENGINE_SOURCES
    src/*.cpp
)

add_executable(LSMTreeEngine ${ENGINE_SOURCES})

target_include_directories(LSMTreeEngine PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)

# Windows-specific settings
if (MSVC)
    target_compile_options(LSMTreeEngine PRIVATE /W4)
else()
    target_compile_options(LSMTreeEngine PRIVATE -Wall -Wextra -Wpedantic)
endif()