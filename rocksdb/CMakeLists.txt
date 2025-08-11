cmake_minimum_required(VERSION 4.0)
project(rockseb_bench)

set(CMAKE_CXX_STANDARD 23)

add_executable(rockseb_bench main.cpp
        instant.h)
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
target_compile_options(rockseb_bench PRIVATE "-flto")
target_link_libraries(rockseb_bench rocksdb)
