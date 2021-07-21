cmake_minimum_required (VERSION 3.12.0)

project(tools)

include(CheckIPOSupported)
check_ipo_supported()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
endif()

option(NOLOG "Disables logging in Paxos client programs" ON)
if(NOLOG)
    message(STATUS "Paxos client programs verbose logging is DISABLED")
    add_definitions(-DNOLOG)
else()
    message(STATUS "Paxos client programs verbose logging is ENABLED")
endif()

add_compile_options(-Wall -Wextra)

add_executable(systemStats systemStats.cpp)
target_link_libraries(systemStats rt)

add_executable(systemStats2 systemStats2.cpp)
target_link_libraries(systemStats2 rt)

add_executable(jpaxosMClient jpaxosMClient.cpp)

add_executable(hashMapClient jpaxosMClient.cpp hashMapClient.cpp)

add_executable(sigcatcher sigcatcher.cpp)

add_executable(fileWriter fileWriter.cpp)
