cmake_minimum_required(VERSION 3.20 FATAL_ERROR)
project(NanoGDR LANGUAGES CUDA CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-unused-parameter -Wno-unused-function")

option(BUILD_TESTS "Build tests" ON)
option(BUILD_EXAMPLES "Build examples" ON)
option(BUILD_BENCHMARKS "Build benchmarks" ON)
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(USE_NCCL "Link benchmarks with NCCL" OFF)

option(ENABLE_BUSY_WAIT "Enable busy-wait" OFF)
if(ENABLE_BUSY_WAIT)
    add_compile_definitions(BUSY_WAIT)
endif()

# Add third-party libraries
if(NOT TARGET fmt)
    add_subdirectory(third_party/fmt)
    link_libraries(fmt-header-only)
endif()

if(NOT TARGET concurrentqueue)
    add_subdirectory(third_party/concurrentqueue)
endif()

if(BUILD_TESTS)
    add_subdirectory(third_party/googletest)
endif()

enable_language(CUDA)
find_package(CUDAToolkit REQUIRED)

if(USE_NCCL)
    # Locate NCCL to allow benchmark builds to compare against NCCL performance.
    find_library(NCCL_LIBRARY nccl
        HINTS ${CUDAToolkit_LIBRARY_DIR} ${CUDAToolkit_LIBRARY_ROOT}
        PATH_SUFFIXES lib lib64
    )
    find_path(NCCL_INCLUDE_DIR nccl.h
        HINTS ${CUDAToolkit_INCLUDE_DIRS} ${CUDAToolkit_LIBRARY_ROOT}/include /usr/local/cuda/include
    )
    if(NOT NCCL_LIBRARY OR NOT NCCL_INCLUDE_DIR)
        message(FATAL_ERROR "USE_NCCL is ON but NCCL could not be located")
    endif()
endif()

# Create rdma_util library
add_library(rdma_util "src/rdma_util.cpp")
target_link_libraries(rdma_util PUBLIC ibverbs)
target_include_directories(rdma_util PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

add_library(pickle "src/executor_rdma.cpp" "src/executor_nvlink.cpp" "src/pickle.cpp")

target_link_libraries(pickle PUBLIC rdma_util concurrentqueue CUDA::cudart)

target_include_directories(pickle PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# Add test executables
if(BUILD_TESTS)
    file(GLOB TEST_SOURCES "tests/*.cpp")

    foreach(TEST_SOURCE ${TEST_SOURCES})
        get_filename_component(TEST_NAME ${TEST_SOURCE} NAME_WE)
        add_executable(${TEST_NAME} ${TEST_SOURCE})
        target_link_libraries(${TEST_NAME} gtest_main rdma_util pickle CUDA::cudart)
        add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
    endforeach()
endif()

# Add example executables
if(BUILD_EXAMPLES)
    file(GLOB EXAMPLE_SOURCES "examples/*.cpp")

    foreach(EXAMPLE_SOURCE ${EXAMPLE_SOURCES})
        get_filename_component(EXAMPLE_NAME ${EXAMPLE_SOURCE} NAME_WE)
        add_executable(${EXAMPLE_NAME} ${EXAMPLE_SOURCE})
        target_link_libraries(${EXAMPLE_NAME} rdma_util pickle CUDA::cudart)
    endforeach()
endif()

# Add benchmark executables
if(BUILD_BENCHMARKS)
    find_package(MPI REQUIRED)
    file(GLOB BENCHMARK_SOURCES "benchmarks/*.cpp")

    foreach(BENCHMARK_SOURCE ${BENCHMARK_SOURCES})
        get_filename_component(BENCHMARK_NAME ${BENCHMARK_SOURCE} NAME_WE)
        add_executable(${BENCHMARK_NAME} ${BENCHMARK_SOURCE})
        target_link_libraries(${BENCHMARK_NAME} rdma_util pickle pthread CUDA::cudart)
        if(BENCHMARK_NAME STREQUAL "bench_nccl_p2p" OR BENCHMARK_NAME STREQUAL "bench_pickle_p2p")
            target_link_libraries(${BENCHMARK_NAME} MPI::MPI_CXX)
        endif()
        if(USE_NCCL)
            target_link_libraries(${BENCHMARK_NAME} ${NCCL_LIBRARY})
            target_include_directories(${BENCHMARK_NAME} PRIVATE ${NCCL_INCLUDE_DIR})
            target_compile_definitions(${BENCHMARK_NAME} PRIVATE USE_NCCL)
        endif()
    endforeach()
endif()

# Export the library for use with add_subdirectory
install(TARGETS rdma_util concurrentqueue
    EXPORT NanoGDRTargets
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include
)

export(EXPORT NanoGDRTargets FILE NanoGDRTargets.cmake)
