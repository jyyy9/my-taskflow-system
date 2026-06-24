cmake_minimum_required(VERSION 3.15)
project(TaskFlow VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)

find_program(PROTOBUF_PROTOC_EXECUTABLE protoc)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)

if(NOT GRPC_CPP_PLUGIN)
    message(FATAL_ERROR "grpc_cpp_plugin not found. Please install grpc-dev package.")
endif()

find_path(GRPC_INCLUDE_DIR grpcpp/grpcpp.h
    PATHS /usr/include /usr/local/include
)

find_library(GRPC_LIBRARY
    NAMES grpc
    PATHS /usr/lib /usr/local/lib
)

find_library(GRPCPP_LIBRARY
    NAMES grpc++
    PATHS /usr/lib /usr/local/lib
)

find_library(GPR_LIBRARY
    NAMES gpr
    PATHS /usr/lib /usr/local/lib
)

if(NOT GRPC_INCLUDE_DIR OR NOT GRPCPP_LIBRARY)
    message(FATAL_ERROR "gRPC not found. Please install libgrpc++-dev package.")
endif()

set(GRPC_INCLUDE_DIRS ${GRPC_INCLUDE_DIR})
set(GRPC_LIBRARIES ${GRPCPP_LIBRARY} ${GRPC_LIBRARY} ${GPR_LIBRARY})

message(STATUS "Found gRPC: ${GRPCPP_LIBRARY}")

find_path(BOOST_INCLUDE_DIRS "boost/asio.hpp")

find_package(yaml-cpp QUIET)
find_package(spdlog QUIET)

if(NOT yaml-cpp_FOUND)
    find_path(YAML_CPP_INCLUDE_DIRS "yaml-cpp/yaml.h")
    find_library(YAML_CPP_LIBRARY NAMES yaml-cpp)
    if(YAML_CPP_INCLUDE_DIRS AND YAML_CPP_LIBRARY)
        set(yaml-cpp_FOUND TRUE)
        message(STATUS "Found yaml-cpp: ${YAML_CPP_LIBRARY}")
    endif()
endif()

if(NOT spdlog_FOUND)
    find_path(SPDLOG_INCLUDE_DIRS "spdlog/spdlog.h")
    find_library(SPDLOG_LIBRARY NAMES spdlog)
    if(SPDLOG_INCLUDE_DIRS AND SPDLOG_LIBRARY)
        set(spdlog_FOUND TRUE)
        message(STATUS "Found spdlog: ${SPDLOG_LIBRARY}")
    endif()
endif()

if(NOT yaml-cpp_FOUND OR NOT spdlog_FOUND)
    message(STATUS "Some dependencies not found, using FetchContent...")
    include(FetchContent)

    if(NOT spdlog_FOUND)
        FetchContent_Declare(
            spdlog
            GIT_REPOSITORY https://github.com/gabime/spdlog.git
            GIT_TAG v1.11.0
        )
        FetchContent_MakeAvailable(spdlog)
    endif()

    if(NOT yaml-cpp_FOUND)
        FetchContent_Declare(
            yaml_cpp
            GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
            GIT_TAG yaml-cpp-0.7.0
        )
        FetchContent_MakeAvailable(yaml_cpp)
    endif()
endif()

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${Protobuf_INCLUDE_DIRS}
    ${GRPC_INCLUDE_DIRS}
    ${BOOST_INCLUDE_DIRS}
    ${CMAKE_CURRENT_BINARY_DIR}
)

if(SPDLOG_INCLUDE_DIRS)
    include_directories(${SPDLOG_INCLUDE_DIRS})
endif()

if(YAML_CPP_INCLUDE_DIRS)
    include_directories(${YAML_CPP_INCLUDE_DIRS})
endif()

file(GLOB PROTO_FILES ${CMAKE_CURRENT_SOURCE_DIR}/proto/*.proto)
file(GLOB COMMON_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/common/*.h)
file(GLOB COMMON_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/src/common/*.cpp)

set(PROTO_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto)
file(MAKE_DIRECTORY ${PROTO_OUTPUT_DIR})

set(PROTO_SRCS "")
set(PROTO_HDRS "")

foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    get_filename_component(PROTO_PATH ${PROTO_FILE} PATH)
    
    set(PROTO_SRC ${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.cc)
    set(PROTO_HDR ${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.h)
    set(GRPC_SRC ${PROTO_OUTPUT_DIR}/${PROTO_NAME}.grpc.pb.cc)
    set(GRPC_HDR ${PROTO_OUTPUT_DIR}/${PROTO_NAME}.grpc.pb.h)
    
    list(APPEND PROTO_SRCS ${PROTO_SRC} ${GRPC_SRC})
    list(APPEND PROTO_HDRS ${PROTO_HDR} ${GRPC_HDR})
    
    add_custom_command(
        OUTPUT ${PROTO_SRC} ${PROTO_HDR} ${GRPC_SRC} ${GRPC_HDR}
        COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
        ARGS --proto_path=${PROTO_PATH}
             --cpp_out=${PROTO_OUTPUT_DIR}
             --grpc_out=${PROTO_OUTPUT_DIR}
             --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
             ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
        COMMENT "Generating gRPC code from ${PROTO_FILE}"
    )
endforeach()

add_library(taskflow_proto STATIC ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(taskflow_proto ${GRPC_LIBRARIES} ${Protobuf_LIBRARIES})

add_library(taskflow_common STATIC
    ${COMMON_HEADERS}
    ${COMMON_SOURCES}
)
target_link_libraries(taskflow_common
    Threads::Threads
    ${Protobuf_LIBRARIES}
    ${GRPC_LIBRARIES}
    taskflow_proto
    hiredis
    curl
)

if(TARGET spdlog::spdlog)
    target_link_libraries(taskflow_common spdlog::spdlog)
elseif(SPDLOG_LIBRARY)
    target_link_libraries(taskflow_common ${SPDLOG_LIBRARY})
endif()

if(TARGET yaml-cpp)
    target_link_libraries(taskflow_common yaml-cpp)
elseif(YAML_CPP_LIBRARY)
    target_link_libraries(taskflow_common ${YAML_CPP_LIBRARY})
endif()

set(GATEWAY_SOURCES
    src/gateway.cpp
    src/gateway/http_handler.cpp
)
add_executable(gateway ${GATEWAY_SOURCES})
add_dependencies(gateway taskflow_proto)
target_link_libraries(gateway taskflow_common pthread)

set(SCHEDULER_SOURCES
    src/scheduler.cpp
    src/scheduler/load_balancer.cpp
    src/scheduler/task_dispatcher.cpp
)
add_executable(scheduler ${SCHEDULER_SOURCES})
add_dependencies(scheduler taskflow_proto)
target_link_libraries(scheduler taskflow_common pthread)

set(WORKER_SOURCES
    src/worker.cpp
)
add_executable(worker ${WORKER_SOURCES})
add_dependencies(worker taskflow_proto)
target_link_libraries(worker taskflow_common pthread)

set(TRACKER_SOURCES
    src/tracker.cpp
)
add_executable(tracker ${TRACKER_SOURCES})
add_dependencies(tracker taskflow_proto)
target_link_libraries(tracker taskflow_common pthread)

set(STATS_SOURCES
    src/stats.cpp
)
add_executable(stats ${STATS_SOURCES})
add_dependencies(stats taskflow_proto)
target_link_libraries(stats taskflow_common pthread)

enable_testing()
add_test(NAME TaskFlowTest COMMAND echo "Tests placeholder")
