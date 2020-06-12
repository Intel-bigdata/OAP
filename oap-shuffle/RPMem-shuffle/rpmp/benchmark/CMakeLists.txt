add_executable(local_allocate local_allocate.cc)
target_link_libraries(local_allocate pmpool)

add_executable(remote_allocate remote_allocate.cc)
target_link_libraries(remote_allocate pmpool)

add_executable(remote_write remote_write.cc)
target_link_libraries(remote_write pmpool)

add_executable(remote_allocate_write remote_allocate_write.cc)
target_link_libraries(remote_allocate_write pmpool)

add_executable(circularbuffer circularbuffer.cc)
target_link_libraries(circularbuffer pmpool)

add_executable(remote_read remote_read.cc)
target_link_libraries(remote_read pmpool)
