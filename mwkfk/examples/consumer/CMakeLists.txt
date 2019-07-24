set(consumer_SRCS
	mwkfk_consumer_example.cc
  )

include_directories(${PROJECT_SOURCE_DIR}/mwkfk/examples/include)
link_directories(${PROJECT_SOURCE_DIR}/mwkfk/examples/lib)

add_executable(consumer_test mwkfk_consumer_example.cc)
target_link_libraries(consumer_test
    mwkfk
    mwnet_mt_util
    mwnet_mt_base
    mwnet_mt_netbase
	mwlogger
    sasl2
    stdc++
    pthread
    z
    rt
    dl
    )