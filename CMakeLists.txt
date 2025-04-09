cmake_minimum_required(VERSION 3.10)
project(parquet_writer)

# Point to conda-installed Arrow/Parquet headers & libs
include_directories("$ENV{CONDA_PREFIX}/include/arrow")
include_directories("$ENV{CONDA_PREFIX}/include/parquet")
include_directories(${CMAKE_SOURCE_DIR})  # for k.h
link_directories("$ENV{CONDA_PREFIX}/lib")

add_library(parquet_writer SHARED writer.cpp c.o)
target_link_libraries(parquet_writer arrow parquet arrow_dataset)
file(COPY ${CMAKE_SOURCE_DIR}/parquet.q DESTINATION ${CMAKE_BINARY_DIR})
file(COPY ${CMAKE_SOURCE_DIR}/demo_write_parquet.q DESTINATION ${CMAKE_BINARY_DIR})
file(COPY ${CMAKE_SOURCE_DIR}/trades.csv DESTINATION ${CMAKE_BINARY_DIR})
