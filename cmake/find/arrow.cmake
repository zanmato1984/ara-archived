message(STATUS "Finding Arrow")

find_package(Arrow REQUIRED)

if(Arrow_FOUND)
  message(STATUS "Arrow found")
else()
  message(FATAL_ERROR "Arrow not found")
endif(Arrow_FOUND)