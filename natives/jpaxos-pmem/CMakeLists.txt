cmake_minimum_required (VERSION 3.0.0)

project(jpaxos-pmem)

set(CMAKE_CXX_STANDARD 17)

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
endif()

add_compile_options(-Wall -Wextra)

find_package(JNI)

if(NOT JNI_FOUND)
        message(FATAL_ERROR "JNI not found")
endif()

include_directories(${JNI_INCLUDE_DIRS} ..)

add_library          (jpaxos-pmem SHARED 
                        jpaxos-pmem.cpp
                        jpaxos-common.cpp
                        consensusinstance.cpp
                        consensuslog.cpp
                        paxosstorage.cpp
                        replicastorage.cpp
                        serviceproxystorage.cpp
                        )
target_link_libraries(jpaxos-pmem pmem pmemobj)

add_executable(dumper dumper.cpp)
target_link_libraries(dumper jpaxos-pmem)
