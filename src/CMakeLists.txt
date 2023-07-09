cmake_minimum_required(VERSION 3.9)
project(src)

set(SOURCE_FILES
        main.cc
        block_builder.h
        block_builder.cc
        block.cc
        block.h
        iterator.cc

        ../util/coding.h
        ../util/coding.cc
        ../util/status.cc
        ../util/options.cc
        ../util/comparator.cc
        ../util/env_posix.cc
        ../util/crc32c.h
        ../util/crc32c.cc
        ../util/env.cc

        ../include/options.h
        ../include/slice.h
        ../include/comparator.h
        ../include/env.h
        ../include/options.h
        ../include/iterator.h

        ../port/port_config.h.in
        ../port/port_stdcxx.h
        ../port/port.h
        ../port/port_config.h

        table_builder.cc
        table_builder.h

        format.h
        format.cc
        table_builder.h
        table_builder.cc

        table.cc
        table.h
        )

add_executable(src ${SOURCE_FILES})
set(ROS_BUILD_TYPE Debug)


include(CheckLibraryExists)
# ubuntu 安装 snappy1.1.7 https://blog.csdn.net/qq_36835255/article/details/124708084
# /usr/local/include 中包含snappy.h
link_directories("/usr/local/include")
# /usr/local/lib 中包含libsnappy.a
include_directories("/usr/local/lib" "/usr/local/include")
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)

set(HAVE_SNAPPY ON)

if (HAVE_SNAPPY)
    target_link_libraries(src snappy)
endif (HAVE_SNAPPY)

target_link_libraries(src pthread)