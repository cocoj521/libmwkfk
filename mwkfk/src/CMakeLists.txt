set(mwkfk_SRCS
    ../util/logger.cc
    mwkfk_helper.cc
    mwkfk_config.cc
    mwkfk_consumer_imp.cc
    mwkfk_consumer.cc
    mwkfk_producer.cc
  )

set(HEADERS
    ../util/logger.h
    mwkfk_helper.h
    mwkfk_config.h
    mwkfk_consumer_imp.h
    mwkfk_consumer.h
    mwkfk_producer.h
  )

if(NOT CMAKE_BUILD_DYNAMIC_LIB)
	add_library(mwkfk ${mwkfk_SRCS})
else()
	add_library(mwkfk SHARED ${mwkfk_SRCS})
endif()

target_link_libraries(mwkfk 
  log4cplus 
  rdkafka
  ssl
  crypto
  )

install(TARGETS mwkfk DESTINATION lib)

install(FILES ${HEADERS} DESTINATION include/mwkfk)

#add_subdirectory(tests)