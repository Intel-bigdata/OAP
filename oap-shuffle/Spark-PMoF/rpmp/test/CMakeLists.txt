add_executable(unit_tests unit_test/main.cc unit_test/DigestTest.cc unit_test/CircularBufferTest.cc)
target_link_libraries(unit_tests gtest_main pmpool)

add_test(NAME unit_tests COMMAND unit_tests)

add_executable(RemoteRead integration_test/RemoteRead.cc)
target_link_libraries(RemoteRead pmpool)
