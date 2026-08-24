cmake_minimum_required(VERSION 3.16)

set(FUZZER_SOURCES
    ReferenceEngine.cpp
    ExpressionFuzzer.cpp
    JoinFuzzer.cpp
    AggregationFuzzer.cpp
    MemoryFuzzer.cpp
    WriteFuzzer.cpp
    PlanFuzzer.cpp
)

add_library(pangrova_fuzzer STATIC ${FUZZER_SOURCES})

target_include_directories(pangrova_fuzzer PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

set_target_properties(pangrova_fuzzer PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# DuckDB as reference engine for fuzzer result comparison
find_package(DuckDB QUIET)
if(DuckDB_FOUND)
    target_link_libraries(pangrova_fuzzer PUBLIC DuckDB::DuckDB)
else()
    # Fetch DuckDB if not installed system-wide
    include(FetchContent)
    FetchContent_Declare(
        duckdb
        GIT_REPOSITORY https://github.com/duckdb/duckdb.git
        GIT_TAG        v1.1.0
        GIT_SHALLOW    TRUE
    )
    set(BUILD_SHELL OFF CACHE BOOL "" FORCE)
    set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
    set(BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(duckdb)
    target_link_libraries(pangrova_fuzzer PUBLIC duckdb)
    target_include_directories(pangrova_fuzzer PUBLIC
        ${duckdb_SOURCE_DIR}/src/include
    )
endif()

# Fuzzer runner executable (for CI time-bounded execution)
add_executable(pangrova_fuzzer_runner FuzzerRunner.cpp)
target_link_libraries(pangrova_fuzzer_runner PRIVATE pangrova_fuzzer)
set_target_properties(pangrova_fuzzer_runner PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)
