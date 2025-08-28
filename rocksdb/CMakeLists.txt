cmake_minimum_required(VERSION 3.20)
project(rocksdb_bench CXX)

set(CMAKE_CXX_STANDARD 23)

if (CMAKE_BUILD_TYPE MATCHES Release)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto")
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

find_package(CLI11 CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)

add_executable(rocksdb_bench main.cpp
        instant.h)

target_include_directories(rocksdb_bench PRIVATE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(rocksdb_bench PRIVATE RocksDB::rocksdb CLI11::CLI11 fmt::fmt)

file(GLOB ALL_SOURCE_FILES *.cpp *.h)
add_custom_target(format
  COMMAND clang-format --style=file -i ${ALL_SOURCE_FILES}
  VERBATIM
)

add_custom_target(tidy
    COMMAND clang-tidy -p=${CMAKE_BINARY_DIR} --config-file=${CMAKE_SOURCE_DIR}/.clang-tidy ${ALL_SOURCE_FILES} -extra-arg='--std=c++23'
    COMMENT "Running Clang-Tidy"
    VERBATIM
)
