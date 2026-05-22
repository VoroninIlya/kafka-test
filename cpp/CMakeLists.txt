cmake_minimum_required(VERSION 3.22)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

project(kafka-test LANGUAGES C CXX ASM)

set(CMAKE_VERBOSE_MAKEFILE ON)

add_executable(kafka-consumer 
  main-consumer.cpp

  threadpool/ThreadPool.cpp
  registry/Registry.cpp
  workers/consumer/ConsumerWorker.cpp
)

target_include_directories(kafka-consumer  PRIVATE 
  ${CMAKE_CURRENT_SOURCE_DIR}/threadpool
  ${CMAKE_CURRENT_SOURCE_DIR}/registry
  ${CMAKE_CURRENT_SOURCE_DIR}/workers/interface
  ${CMAKE_CURRENT_SOURCE_DIR}/workers/consumer
)

target_link_libraries(kafka-consumer PRIVATE rdkafka++ rdkafka pthread)

add_executable(kafka-producer 
  main-producer.cpp
  #producer/producer.cpp
  threadpool/ThreadPool.cpp
  registry/Registry.cpp
  workers/producer/ProducerWorker.cpp
)

target_include_directories(kafka-producer PRIVATE 
  ${CMAKE_CURRENT_SOURCE_DIR}/threadpool
  ${CMAKE_CURRENT_SOURCE_DIR}/registry
  ${CMAKE_CURRENT_SOURCE_DIR}/workers/interface
  ${CMAKE_CURRENT_SOURCE_DIR}/workers/producer
)

target_link_libraries(kafka-producer PRIVATE rdkafka++ rdkafka pthread)