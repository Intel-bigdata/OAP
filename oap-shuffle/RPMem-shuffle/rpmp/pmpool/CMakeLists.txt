add_library(pmpool SHARED DataServer.cc Protocol.cc Event.cc NetworkServer.cc hash/xxhash.cc client/PmPoolClient.cc client/NetworkClient.cc client/native/com_intel_rpmp_PmPoolClient.cc)
target_link_libraries(pmpool LINK_PUBLIC ${Boost_LIBRARIES} hpnl pmemobj)
set_target_properties(pmpool PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

if(UNIX AND NOT APPLE)
  set(LINUX TRUE)
endif()

if(APPLE)
  set(JNI_INCLUDE "$ENV{JAVA_HOME}/include" "$ENV{JAVA_HOME}/include/darwin")
endif()
if(LINUX)
  set(JNI_INCLUDE "$ENV{JAVA_HOME}/include" "$ENV{JAVA_HOME}/include/linux")
endif()
include_directories(${JNI_INCLUDE})

set(CMAKE_INSTALL_PREFIX "/usr/local")
install(TARGETS pmpool LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
