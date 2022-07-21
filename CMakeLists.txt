cmake_minimum_required(VERSION 3.16)

project(ARA VERSION 0.0.1 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

option(ENABLE_TESTS "Configure CMake to build tests" ON)

if(ENABLE_ASAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
endif(ENABLE_ASAN)

include(cmake/find/arrow.cmake)
include(cmake/find/rapidjson.cmake)
include(cmake/find/gtest.cmake)

add_library(ara
    "${CMAKE_SOURCE_DIR}/src/c/api.cpp"
    "${CMAKE_SOURCE_DIR}/src/c/bridge.cpp"
    "${CMAKE_SOURCE_DIR}/src/data/column_factories.cpp"
    "${CMAKE_SOURCE_DIR}/src/data/fragment.cpp"
    "${CMAKE_SOURCE_DIR}/src/driver/driver.cpp"
    "${CMAKE_SOURCE_DIR}/src/driver/option.cpp"
    "${CMAKE_SOURCE_DIR}/src/execution/context.cpp"
    "${CMAKE_SOURCE_DIR}/src/execution/executor.cpp"
    "${CMAKE_SOURCE_DIR}/src/execution/memory_resource.cpp"
    "${CMAKE_SOURCE_DIR}/src/execution/pipeline.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/aggregation.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/binary_op.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/expressions.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/literal.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/ti_unary_op.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/unary_op.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/aggregate.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/aggregate_helper.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/bucket_aggregate.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/filter.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/hash_join.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/helper.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/kernel.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/limit.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/project.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/sort.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/sources.cpp"
    "${CMAKE_SOURCE_DIR}/src/kernel/unions.cpp"
    "${CMAKE_SOURCE_DIR}/src/planning/explainer.cpp"
    "${CMAKE_SOURCE_DIR}/src/planning/hash_join_breaker.cpp"
    "${CMAKE_SOURCE_DIR}/src/planning/pipeline_generator.cpp"
    "${CMAKE_SOURCE_DIR}/src/planning/planner.cpp"
    "${CMAKE_SOURCE_DIR}/src/planning/validators.cpp"
    "${CMAKE_SOURCE_DIR}/src/relational/parsers.cpp"
    "${CMAKE_SOURCE_DIR}/src/type/data_type.cpp"
    "${CMAKE_SOURCE_DIR}/src/expression/extract_year.cpp")

include_directories("${CMAKE_SOURCE_DIR}/include" "${RAPIDJSON_INCLUDE_DIRS}")
target_link_libraries(ara PRIVATE arrow_shared)

if(ENABLE_TESTS)
  add_subdirectory(${CMAKE_SOURCE_DIR}/tests)
#  include(CTest)
#
#  if(GTEST_FOUND)
#    message(STATUS "Google C++ Testing Framework (Google Test) found in ${GTEST_ROOT}")
#    include_directories(${GTEST_INCLUDE_DIR})
#  else()
#    message(AUTHOR_WARNING "Google C++ Testing Framework (Google Test) not found: automated tests are disabled")
#  endif(GTEST_FOUND)
endif(ENABLE_TESTS)

message(STATUS "ARA_TEST_LIST set to: ${CURA_TEST_LIST}")
