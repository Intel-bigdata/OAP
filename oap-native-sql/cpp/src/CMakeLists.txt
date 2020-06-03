cmake_minimum_required(VERSION 3.11)

project(spark_columnar_jni)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)

# This ensures that things like gnu++11 get passed correctly
set(CMAKE_CXX_STANDARD 14)

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_BUILD_TYPE  "Release")

option(TESTS "Build the tests" OFF)
option(BENCHMARKS "Build the benchmarks" OFF)
option(DEBUG "Enable Debug Info" OFF)

find_package(JNI REQUIRED)
set(source_root_directory ${CMAKE_CURRENT_SOURCE_DIR})

# Gandiva protobuf

set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
find_package(Protobuf)

if ("${Protobuf_LIBRARY}" STREQUAL "Protobuf_LIBRARY-NOTFOUND")
  message(WARNING "libprotobuf.so not found, will build from source")
  set(BUILD_PROTOBUF true)
endif()

if(BUILD_PROTOBUF)
  message(STATUS "Building Protocol Buffers from source")
  set (PROTOBUF_SOURCE_URL
       "https://github.com/protocolbuffers/protobuf/releases/download/v3.7.1/protobuf-all-3.7.1.tar.gz"
        "https://github.com/ursa-labs/thirdparty/releases/download/latest/protobuf-v3.7.1.tar.gz"
)
  set(PROTOBUF_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/protobuf_ep-install")
  set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_PREFIX}/include")
  set(
    PROTOBUF_STATIC_LIB
    "${PROTOBUF_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    PROTOC_STATIC_LIB
    "${PROTOBUF_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protoc${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    PROTOC_BIN
    "${PROTOBUF_PREFIX}/bin/protoc"
    )
  set(
    PROTOBUF_INCLUDE
    "${PROTOBUF_PREFIX}/include"
    )
  set(PROTOBUF_COMPILER "${PROTOBUF_PREFIX}/bin/protoc")
  set(PROTOBUF_CONFIGURE_ARGS
      "AR=${CMAKE_AR}"
      "RANLIB=${CMAKE_RANLIB}"
      "CC=${CMAKE_C_COMPILER}"
      "CXX=${CMAKE_CXX_COMPILER}"
      "--disable-shared"
      "--prefix=${PROTOBUF_PREFIX}"
      "CFLAGS=-fPIC"
      "CXXFLAGS=-fPIC")
  set(PROTOBUF_BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS})
  ExternalProject_Add(protobuf_ep
                      PREFIX protobuf_ep
                      CONFIGURE_COMMAND "./configure" ${PROTOBUF_CONFIGURE_ARGS}
                      BUILD_COMMAND ${PROTOBUF_BUILD_COMMAND}
                      BUILD_IN_SOURCE 1
                      URL_MD5 cda6ae370a5df941f8aa837c8a0292ba
                      URL ${PROTOBUF_SOURCE_URL}
  )

  file(MAKE_DIRECTORY "${PROTOBUF_INCLUDE_DIR}")
  add_library(protobuf::libprotobuf STATIC IMPORTED)
  set_target_properties(
    protobuf::libprotobuf
    PROPERTIES IMPORTED_LOCATION "${PROTOBUF_STATIC_LIB}" INTERFACE_INCLUDE_DIRECTORIES
               "${PROTOBUF_INCLUDE_DIR}")
  add_dependencies(protobuf::libprotobuf protobuf_ep)
else()
  set(PROTOC_BIN ${Protobuf_PROTOC_EXECUTABLE})
endif()

file(MAKE_DIRECTORY ${root_directory}/src/proto)
set(PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto")
set(PROTO_OUTPUT_FILES "${PROTO_OUTPUT_DIR}/Exprs.pb.cc")
set(PROTO_OUTPUT_FILES ${PROTO_OUTPUT_FILES} "${PROTO_OUTPUT_DIR}/Exprs.pb.h")

set_source_files_properties(${PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

get_filename_component(ABS_GANDIVA_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/proto/Exprs.proto
                       ABSOLUTE)

add_custom_command(OUTPUT ${PROTO_OUTPUT_FILES}
                   COMMAND ${PROTOC_BIN}
                           --proto_path
                           ${CMAKE_CURRENT_SOURCE_DIR}/proto
                           --cpp_out
                       ${PROTO_OUTPUT_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR}/proto/Exprs.proto
                   DEPENDS  ${ABS_GANDIVA_PROTO}
                   COMMENT "Running PROTO compiler on Exprs.proto"
                   VERBATIM)

add_custom_target(jni_proto ALL DEPENDS ${PROTO_OUTPUT_FILES})
set(PROTO_SRCS "${PROTO_OUTPUT_DIR}/Exprs.pb.cc")
set(PROTO_HDRS "${PROTO_OUTPUT_DIR}/Exprs.pb.h")
set(GANDIVA_LINK_LIBS gandiva)

#######
if(TESTS)
  find_package(GTest)
macro(package_add_test TESTNAME)
  add_executable(${TESTNAME} ${ARGN})
  target_link_libraries(${TESTNAME} gtest gmock gtest_main spark_columnar_jni ${CMAKE_THREAD_LIBS_INIT})
  target_include_directories(${TESTNAME} PUBLIC ${source_root_directory})
  gtest_discover_tests(${TESTNAME}
    WORKING_DIRECTORY ${PROJECT_DIR}
    PROPERTIES VS_DEBUGGER_WORKING_DIRECTORY "${PROJECT_DIR}"
  )
  set_target_properties(${TESTNAME} PROPERTIES FOLDER tests)
endmacro()
  include(GoogleTest)
  ENABLE_TESTING()
  add_custom_target(test ${CMAKE_CTEST_COMMAND} -R TestArrowCompute --output-on-failure)
  add_subdirectory(tests)
endif()

if(BENCHMARKS)
  find_package(GTest)
  #add_definitions(-DBENCHMARK_FILE_PATH=${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/source_files/)
  add_compile_definitions(BENCHMARK_FILE_PATH="file://${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/source_files/")
macro(package_add_benchmark TESTNAME)
  #configure_file(${ARGN}.in ${ARGN})
  add_executable(${TESTNAME} ${ARGN})
  target_link_libraries(${TESTNAME} gtest gmock gtest_main spark_columnar_jni parquet ${CMAKE_THREAD_LIBS_INIT})
  target_include_directories(${TESTNAME} PUBLIC ${source_root_directory})
  gtest_discover_tests(${TESTNAME}
    WORKING_DIRECTORY ${PROJECT_DIR}
    PROPERTIES VS_DEBUGGER_WORKING_DIRECTORY "${PROJECT_DIR}"
  )
  set_target_properties(${TESTNAME} PROPERTIES FOLDER tests)
endmacro()
  include(GoogleTest)
  ENABLE_TESTING()
  add_custom_target(benchmark ${CMAKE_CTEST_COMMAND} -R BenchmarkArrowCompute --output-on-failure)
  add_subdirectory(benchmarks)
endif()
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mtune=native")
if(DEBUG)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -DDEBUG -DDEBUG_LEVEL_1 -DDEBUG_LEVEL_2")
endif()
find_library(ARROW_LIB arrow)
find_library(PARQUET_LIB parquet)
find_library(GANDIVA_LIB gandiva)

if(NOT ARROW_LIB)
    message(FATAL_ERROR "Arrow library not found")
endif()

if(NOT GANDIVA_LIB)
    message(FATAL_ERROR "Gandiva library not found")
endif()

set(SPARK_COLUMNAR_PLUGIN_SRCS
        jni/jni_wrapper.cc
        ${PROTO_SRCS}
        data_source/parquet/adapter.cc
        proto/protobuf_utils.cc
        codegen/expr_visitor.cc
        codegen/arrow_compute/expr_visitor.cc
        codegen/arrow_compute/ext/item_iterator.cc
        codegen/arrow_compute/ext/shuffle_v2_action.cc
        codegen/arrow_compute/ext/codegen_node_visitor.cc
        codegen/arrow_compute/ext/codegen_node_visitor_v2.cc
        codegen/arrow_compute/ext/conditioned_shuffle_kernel.cc
        codegen/arrow_compute/ext/conditioned_probe_kernel.cc
        codegen/arrow_compute/ext/sort_kernel.cc
        codegen/arrow_compute/ext/kernels_ext.cc
        codegen/arrow_compute/ext/codegen_common.cc
        )

file(MAKE_DIRECTORY ${root_directory}/releases)
add_library(spark_columnar_jni SHARED ${SPARK_COLUMNAR_PLUGIN_SRCS})
add_dependencies(spark_columnar_jni jni_proto)
if(BUILD_PROTOBUF)
target_link_libraries(spark_columnar_jni
                      LINK_PUBLIC ${ARROW_LIB} ${PARQUET_LIB} ${GANDIVA_LIB} ${GANDIVA_LINK_LIBS}
                      LINK_PRIVATE protobuf::libprotobuf)
else()
target_link_libraries(spark_columnar_jni
                      LINK_PUBLIC ${ARROW_LIB} ${PARQUET_LIB} ${GANDIVA_LIB} ${GANDIVA_LINK_LIBS} ${PROTOBUF_LIBRARY})
endif()
target_include_directories(spark_columnar_jni PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH} ${JNI_INCLUDE_DIRS} ${source_root_directory} ${PROTO_OUTPUT_DIR} ${PROTOBUF_INCLUDE})
set_target_properties(spark_columnar_jni PROPERTIES
                      LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
)

if(DEFINED ENV{HADOOP_HOME})
  set(LIBHDFS3_DESTINATION $ENV{HADOOP_HOME}/lib/native)
else()
  set(LIBHDFS3_DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

install(TARGETS spark_columnar_jni
        DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES ${source_root_directory}/resources/libhdfs.so
        DESTINATION ${LIBHDFS3_DESTINATION})
install(FILES ${source_root_directory}/resources/libprotobuf.so.13
        DESTINATION ${CMAKE_INSTALL_LIBDIR})
