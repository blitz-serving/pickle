cmake_minimum_required(VERSION 3.5 FATAL_ERROR)
project(NanoGDR LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_BUILD_TYPE Release)

option(BUILD_TESTS "Build tests" ON)
option(BUILD_EXAMPLES "Build examples" ON)
option(BUILD_BENCHMARKS "Build benchmarks" ON)
option(USE_CUDA "Use CUDA" ON)

# Add third-party libraries
add_subdirectory(third_party/concurrentqueue)

if(BUILD_TESTS)
    add_subdirectory(third_party/googletest)
endif()

# Create rdma_util library
add_library(rdma_util STATIC "src/rdma_util.cpp")
target_link_libraries(rdma_util PUBLIC ibverbs concurrentqueue)
target_include_directories(rdma_util PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# Handle CUDA
if(USE_CUDA)
    set(CUDA_TOOLKIT_ROOT_DIR "/usr/local/cuda")
    find_package(CUDA REQUIRED)
    add_library(gpu_mem_util STATIC "src/gpu_mem_util.cpp")
    message(STATUS "CUDA_INCLUDE_DIRS: ${CUDA_INCLUDE_DIRS}")
    message(STATUS "CUDA_LIBRARIES: ${CUDA_LIBRARIES}")
    target_link_libraries(gpu_mem_util PUBLIC ${CUDA_LIBRARIES})
    target_include_directories(gpu_mem_util PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        ${CUDA_INCLUDE_DIRS}
    )
endif()

# Add test executables
if(BUILD_TESTS)
    file(GLOB TEST_SOURCES "tests/*.cpp")

    foreach(TEST_SOURCE ${TEST_SOURCES})
        get_filename_component(TEST_NAME ${TEST_SOURCE} NAME_WE)
        add_executable(${TEST_NAME} ${TEST_SOURCE})

        if(USE_CUDA)
            target_compile_definitions(${TEST_NAME} PRIVATE USE_CUDA)
            target_link_libraries(${TEST_NAME} gtest_main rdma_util gpu_mem_util)
        else()
            target_link_libraries(${TEST_NAME} gtest_main rdma_util)
        endif()

        add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
    endforeach()
endif()

# Add example executables
if(BUILD_EXAMPLES)
    file(GLOB EXAMPLE_SOURCES "examples/*.cpp")

    foreach(EXAMPLE_SOURCE ${EXAMPLE_SOURCES})
        get_filename_component(EXAMPLE_NAME ${EXAMPLE_SOURCE} NAME_WE)
        add_executable(${EXAMPLE_NAME} ${EXAMPLE_SOURCE})

        if(USE_CUDA)
            target_compile_definitions(${EXAMPLE_NAME} PRIVATE USE_CUDA)
            target_link_libraries(${EXAMPLE_NAME} rdma_util gpu_mem_util)
        else()
            target_link_libraries(${EXAMPLE_NAME} rdma_util)
        endif()
    endforeach()
endif()

# Add benchmark executables
if(BUILD_BENCHMARKS)
    file(GLOB BENCHMARK_SOURCES "benchmarks/*.cpp")

    foreach(BENCHMARK_SOURCE ${BENCHMARK_SOURCES})
        get_filename_component(BENCHMARK_NAME ${BENCHMARK_SOURCE} NAME_WE)
        add_executable(${BENCHMARK_NAME} ${BENCHMARK_SOURCE})

        if(USE_CUDA)
            target_compile_definitions(${BENCHMARK_NAME} PRIVATE USE_CUDA)
            target_link_libraries(${BENCHMARK_NAME} rdma_util gpu_mem_util pthread)
        else()
            target_link_libraries(${BENCHMARK_NAME} rdma_util pthread)
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
