cmake_minimum_required(VERSION 3.16)
project(czl_cuckoo)

set(CMAKE_CXX_STANDARD 14)

link_libraries(pthread atomic)

add_executable(table_test table_test.cpp)