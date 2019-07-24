cmake_minimum_required(VERSION 2.6)

project(mwkfk C CXX)

enable_testing()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
endif()

set(CXX_FLAGS
 -g
 # -DVALGRIND
 -DMWNET_MT_STD_STRING
 #-DCHECK_PTHREAD_RETURN_VALUE
 -D_FILE_OFFSET_BITS=64
 -Wall
 -Wextra
 -Werror
 #-Wconversion
 -Wno-unused-parameter
 #-Wold-style-cast
 -Woverloaded-virtual
 -Wpointer-arith
 #-Wshadow
 -Wwrite-strings
 #-march=native
 #-MMD
 -std=c++11
 -fPIC
 -rdynamic
 )
if(CMAKE_BUILD_BITS EQUAL 32)
  list(APPEND CXX_FLAGS "-m32")
endif()
if(CMAKE_BUILD_DYNAMIC_LIB)
  list(APPEND CXX_FLAGS "-shared")
endif()
string(REPLACE ";" " " CMAKE_CXX_FLAGS "${CXX_FLAGS}")

set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_CXX_FLAGS_DEBUG "-O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -finline-limit=1000 -DNDEBUG")
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

find_package(Boost REQUIRED)
find_package(ZLIB REQUIRED)
find_library(BOOSTPO_LIBRARY NAMES boost_program_options)
find_library(TCMALLOC_LIBRARY NAMES tcmalloc_and_profiler)

if(TCMALLOC_INCLUDE_DIR AND TCMALLOC_LIBRARY)
  message(STATUS "found tcmalloc")
endif()

if(ZLIB_FOUND) 
  message(STATUS "found zlib") 
endif()

include_directories(${Boost_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/mwkfk)
include_directories(${PROJECT_SOURCE_DIR}/mwkfk/src)
include_directories(${PROJECT_SOURCE_DIR}/mwkfk/include)

link_directories(${PROJECT_SOURCE_DIR}/mwkfk/lib)

string(TOUPPER ${CMAKE_BUILD_TYPE} BUILD_TYPE)
message(STATUS "CXX_FLAGS = " ${CMAKE_CXX_FLAGS} " " ${CMAKE_CXX_FLAGS_${BUILD_TYPE}})

add_subdirectory(mwkfk/src)
add_subdirectory(mwkfk/examples/consumer)
add_subdirectory(mwkfk/examples/producer)
