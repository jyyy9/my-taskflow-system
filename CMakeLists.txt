cmake_minimum_required(VERSION 3.15)
project(TaskFlow VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(grpc REQUIRED)
find_package(absl REQUIRED)

find_path(SPDLOG_INCLUDE_DIRS "spdlog/spdlog.h")
find_path(YAML_CPP_INCLUDE_DIRS "yaml-cpp/yaml.h")
find_path(BOOST_INCLUDE_DIRS "boost/asio.hpp")

if(NOT SPDLOG_INCLUDE_DIRS)
    message(WARNING "spdlog not found, will use FetchContent")
endif()

if(NOT YAML_CPP_INCLUDE_DIRS)
    message(WARNING "yaml-cpp not found, will use FetchContent")
endif()

include(FetchContent)

FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.12.0
)
FetchContent_Declare(
    yaml_cpp
    GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
    GIT_TAG yaml-cpp-0.8.0
)

FetchContent_MakeAvailable(spdlog yaml_cpp)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${Protobuf_INCLUDE_DIRS}
    ${grpc_INCLUDE_DIRS}
    ${SPDLOG_INCLUDE_DIRS}
    ${YAML_CPP_INCLUDE_DIRS}
    ${BOOST_INCLUDE_DIRS}
)

file(GLOB PROTO_FILES ${CMAKE_CURRENT_SOURCE_DIR}/proto/*.proto)
file(GLOB COMMON_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/common/*.h)
file(GLOB COMMON_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/src/common/*.cpp)

protobuf_generate(
    PROTOS ${PROTO_FILES}
    PROTOS_OUT ${CMAKE_CURRENT_BINARY_DIR}/proto
    PROTOS_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto
)

grpc_generate(
    PROTOS ${PROTO_FILES}
    OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto
    OUT_HEADER_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto
)

add_library(taskflow_common STATIC
    ${COMMON_HEADERS}
    ${COMMON_SOURCES}
)
target_link_libraries(taskflow_common
    Threads::Threads
    spdlog::spdlog
    yaml-cpp
    ${Protobuf_LIBRARIES}
    grpc++
    ${ETCDCPP_LIBRARIES}
)

set(GATEWAY_SOURCES
    src/gateway.cpp
)
add_executable(gateway ${GATEWAY_SOURCES})
target_link_libraries(gateway taskflow_common pthread)

set(SCHEDULER_SOURCES
    src/scheduler.cpp
)
add_executable(scheduler ${SCHEDULER_SOURCES})
target_link_libraries(scheduler taskflow_common pthread)

set(WORKER_SOURCES
    src/worker.cpp
)
add_executable(worker ${WORKER_SOURCES})
target_link_libraries(worker taskflow_common pthread)

set(TRACKER_SOURCES
    src/tracker.cpp
)
add_executable(tracker ${TRACKER_SOURCES})
target_link_libraries(tracker taskflow_common pthread)

set(STATS_SOURCES
    src/stats.cpp
)
add_executable(stats ${STATS_SOURCES})
target_link_libraries(stats taskflow_common pthread)

enable_testing()
add_test(NAME TaskFlowTest COMMAND echo "Tests placeholder")
