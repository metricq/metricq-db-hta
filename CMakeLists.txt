project(metricq-db-hta)

cmake_minimum_required(VERSION 3.8)

include(cmake/DefaultBuildType.cmake)
include(cmake/GitSubmoduleUpdate.cmake)
git_submodule_update()

set(SRCS src/main.cpp src/db.hpp src/db.cpp src/log.cpp)

add_executable(metricq-db-hta ${SRCS})
target_include_directories(metricq-db-hta
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        )
target_compile_features(metricq-db-hta PUBLIC cxx_std_17)
target_compile_options(metricq-db-hta PUBLIC -Wall -Wextra -pedantic)

add_subdirectory(lib/metricq)
add_subdirectory(lib/hta)
add_subdirectory(lib/nitro)

target_link_libraries(metricq-db-hta
        PUBLIC
        metricq::db
        hta::hta
        Nitro::nitro-env
        Nitro::nitro-log
        Nitro::nitro-jiffy
        )

install(TARGETS metricq-db-hta RUNTIME DESTINATION bin)
