cmake_minimum_required(VERSION 3.18 FATAL_ERROR)
project(NanoGDR LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_BUILD_TYPE Release)

option(BUILD_TESTS "Build tests" ON)
option(BUILD_EXAMPLES "Build examples" ON)

include(FetchContent)

if(BUILD_TESTS)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.15.2
    )
    FetchContent_MakeAvailable(googletest)
endif()

add_library(rdma_util STATIC "src/rdma_util.cpp")
target_link_libraries(rdma_util PUBLIC ibverbs mlx5 rdmacm cuda)
target_include_directories(rdma_util PUBLIC "include" "/usr/local/cuda-12.1/targets/x86_64-linux/include" "/usr/local/cuda/include")

# Add test executables
if(BUILD_TESTS)
    file(GLOB TEST_SOURCES "tests/*.cpp")

    foreach(TEST_SOURCE ${TEST_SOURCES})
        get_filename_component(TEST_NAME ${TEST_SOURCE} NAME_WE)
        add_executable(${TEST_NAME} ${TEST_SOURCE})
        target_link_libraries(${TEST_NAME} gtest_main rdma_util)
        add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
    endforeach()
endif()

# Add example executables
if(BUILD_EXAMPLES)
    file(GLOB EXAMPLE_SOURCES "examples/*.cpp")

    foreach(EXAMPLE_SOURCE ${EXAMPLE_SOURCES})
        get_filename_component(EXAMPLE_NAME ${EXAMPLE_SOURCE} NAME_WE)
        add_executable(${EXAMPLE_NAME} ${EXAMPLE_SOURCE})
        target_link_libraries(${EXAMPLE_NAME} ibverbs mlx5 rdmacm rdma_util)
    endforeach()
endif()