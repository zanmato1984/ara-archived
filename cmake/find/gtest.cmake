message(STATUS "Finding GoogleTest")

find_package(GTest REQUIRED)

if(GTest_FOUND)
  message(STATUS "GoogleTest found")
else()
  message(FATAL_ERROR "GoogleTest not found")
endif(GTest_FOUND)