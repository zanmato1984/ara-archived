message(STATUS "Finding RapidJSON")

find_package(RapidJSON REQUIRED)

if(RapidJSON_FOUND)
  message(STATUS "RapidJSON found")
else()
  message(FATAL_ERROR "RapidJSON not found")
endif(RapidJSON_FOUND)