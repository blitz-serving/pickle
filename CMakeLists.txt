cmake_minimum_required(VERSION 3.15 FATAL_ERROR)
project(NanoGDR LANGUAGES CUDA CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_TESTS "Build tests" ON)
option(BUILD_EXAMPLES "Build examples" ON)
option(BUILD_BENCHMARKS "Build benchmarks" ON)
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)

# Add third-party libraries
if(NOT TARGET fmt)
    add_subdirectory(third_party/fmt)
endif()

if(NOT TARGET concurrentqueue)
    add_subdirectory(third_party/concurrentqueue)
endif()

if(BUILD_TESTS)
    add_subdirectory(third_party/googletest)
endif()

# Create rdma_util library
add_library(rdma_util "src/rdma_util.cpp")
target_link_libraries(rdma_util PUBLIC ibverbs concurrentqueue)
target_include_directories(rdma_util PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

add_library(pickle "src/pickle.cpp")
target_link_libraries(pickle PUBLIC rdma_util)
target_include_directories(pickle PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

add_library(gpu_mem_util "src/gpu_mem_util.cpp")
enable_language(CUDA)
find_package(CUDAToolkit)
target_link_libraries(gpu_mem_util PUBLIC CUDA::cudart)
target_link_libraries(gpu_mem_util PUBLIC ${CUDA_LIBRARIES})
target_include_directories(gpu_mem_util PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    ${CUDA_INCLUDE_DIRS}
)

# Add test executables
if(BUILD_TESTS)
    file(GLOB TEST_SOURCES "tests/*.cpp")

    foreach(TEST_SOURCE ${TEST_SOURCES})
        get_filename_component(TEST_NAME ${TEST_SOURCE} NAME_WE)
        add_executable(${TEST_NAME} ${TEST_SOURCE})
        target_link_libraries(${TEST_NAME} gtest_main rdma_util pickle gpu_mem_util)
        add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
    endforeach()
endif()

# Add example executables
if(BUILD_EXAMPLES)
    file(GLOB EXAMPLE_SOURCES "examples/*.cpp")

    foreach(EXAMPLE_SOURCE ${EXAMPLE_SOURCES})
        get_filename_component(EXAMPLE_NAME ${EXAMPLE_SOURCE} NAME_WE)
        add_executable(${EXAMPLE_NAME} ${EXAMPLE_SOURCE})
        target_link_libraries(${EXAMPLE_NAME} rdma_util pickle gpu_mem_util)
    endforeach()
endif()

# Add benchmark executables
if(BUILD_BENCHMARKS)
    file(GLOB BENCHMARK_SOURCES "benchmarks/*.cpp")

    foreach(BENCHMARK_SOURCE ${BENCHMARK_SOURCES})
        get_filename_component(BENCHMARK_NAME ${BENCHMARK_SOURCE} NAME_WE)
        add_executable(${BENCHMARK_NAME} ${BENCHMARK_SOURCE})
        target_link_libraries(${BENCHMARK_NAME} rdma_util pickle gpu_mem_util pthread)
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
