cmake_minimum_required(VERSION 3.22)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

project(kafka-test LANGUAGES C CXX ASM)

set(CMAKE_VERBOSE_MAKEFILE ON)

add_executable(kafka-consumer main-consumer.cpp)

target_link_libraries(kafka-consumer PRIVATE rdkafka++ rdkafka pthread)

add_executable(kafka-producer 
  main-producer.cpp
  producer/producer.cpp
  producer/ThreadPool.cpp
  producer/ProducerWorker.cpp
  producer/ProducerRegistry.cpp
)

target_link_libraries(kafka-producer PRIVATE rdkafka++ rdkafka pthread)