cmake_minimum_required(VERSION 3.8)
project(mustela)

set(CMAKE_CXX_STANDARD 14)

set(SOURCE_FILES
        include/mustela/main.cpp
        include/mustela/mustela.cpp
        include/mustela/mustela.hpp
        include/mustela/pages.cpp
        include/mustela/pages.hpp)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin")

add_executable(mustela ${SOURCE_FILES})