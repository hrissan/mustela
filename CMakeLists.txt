cmake_minimum_required(VERSION 3.8)
project(mustela)

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -Wall -Wextra -Werror=return-type -Wno-unused-parameter")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O0 -Wall -Wextra -Werror=return-type -Wno-unused-parameter")

set(SOURCE_FILES
        include/mustela/bucket.hpp
        include/mustela/bucket.cpp
        include/mustela/cursor.hpp
        include/mustela/cursor.cpp
        include/mustela/db.hpp
        include/mustela/db.cpp
        include/mustela/free_list.hpp
        include/mustela/free_list.cpp
        include/mustela/lock.hpp
        include/mustela/lock.cpp
        include/mustela/main.cpp
        include/mustela/mustela.hpp
        include/mustela/pages.cpp
        include/mustela/pages.hpp
        include/mustela/tx.cpp
        include/mustela/tx.hpp
        include/mustela/utils.cpp
        include/mustela/utils.hpp
        include/mustela/testing.hpp
        include/mustela/testing.cpp
        include/mustela/blake2b.h
        include/mustela/blake2b.c)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin")

add_executable(mustela ${SOURCE_FILES})

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(mustela Threads::Threads)