cmake_minimum_required(VERSION 3.27)
cmake_policy(SET CMP0144 NEW)
project(uredis VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0" CACHE STRING "Debug flags" FORCE)
set(CMAKE_C_FLAGS_DEBUG "-g -O0" CACHE STRING "Debug flags" FORCE)

option(UREDIS_LOGS "Use UREDIS_LOGS" OFF)

add_compile_definitions(DEV_STAGE=${DEV_STAGE})

message(STATUS "DEV_STAGE is set to ${DEV_STAGE}")

include(CTest)
include(FetchContent)

FetchContent_Declare(
        uvent
        GIT_REPOSITORY https://github.com/Usub-development/uvent.git
        GIT_TAG main
)
FetchContent_Declare(
        ureflect
        GIT_REPOSITORY https://github.com/Usub-development/ureflect.git
        GIT_TAG main
)
FetchContent_MakeAvailable(uvent ureflect)

file(GLOB_RECURSE UREDIS_HEADERS CONFIGURE_DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/include/uredis/*.h
        ${CMAKE_CURRENT_SOURCE_DIR}/include/uredis/*.hpp
)
file(GLOB_RECURSE UREDIS_SOURCES CONFIGURE_DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/src/uredis/*.cpp
)

add_library(uredis ${UREDIS_SOURCES} ${UREDIS_HEADERS})
add_library(usub::uredis ALIAS uredis)

target_include_directories(uredis
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/uredis>
)

target_link_libraries(uredis
        PUBLIC
        usub::uvent
        usub::ureflect
)

if (UREDIS_LOGS)
    FetchContent_Declare(
            ulog
            GIT_REPOSITORY https://github.com/Usub-development/ulog.git
            GIT_TAG main
    )
    FetchContent_MakeAvailable(ulog)
    target_link_libraries(uredis
            PUBLIC
            usub::ulog
    )
endif ()

target_compile_definitions(uredis PUBLIC
        $<$<BOOL:${UREDIS_LOGS}>:UREDIS_LOGS>
)

set_target_properties(uredis PROPERTIES
        EXPORT_NAME uredis
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
)

option(UREDIS_BUILD_EXAMPLES "Build uredis example executables" ON)
if (UREDIS_BUILD_EXAMPLES)
    add_executable(uredis_example examples/main.cpp)
    target_link_libraries(uredis_example PRIVATE uredis)
    target_compile_definitions(uredis_example PRIVATE DEV_STAGE=${DEV_STAGE})

    add_executable(uredis_pool_example examples/main_pool.cpp)
    target_link_libraries(uredis_pool_example PRIVATE uredis)
    target_compile_definitions(uredis_pool_example PRIVATE DEV_STAGE=${DEV_STAGE})

    add_executable(uredis_pub_sub_example examples/main_pub_sub.cpp)
    target_link_libraries(uredis_pub_sub_example PRIVATE uredis)
    target_compile_definitions(uredis_pub_sub_example PRIVATE DEV_STAGE=${DEV_STAGE})

    add_executable(uredis_ureflect_example examples/main_ureflect.cpp)
    target_link_libraries(uredis_ureflect_example PRIVATE uredis)
    target_compile_definitions(uredis_ureflect_example PRIVATE DEV_STAGE=${DEV_STAGE})

    add_executable(uredis_bus_example examples/main_bus.cpp)
    target_link_libraries(uredis_bus_example PRIVATE uredis)
    target_compile_definitions(uredis_bus_example PRIVATE DEV_STAGE=${DEV_STAGE})

    add_executable(uredis_sentinel_example examples/main_sentinel_pool.cpp)
    target_link_libraries(uredis_sentinel_example PRIVATE uredis)
    target_compile_definitions(uredis_sentinel_example PRIVATE DEV_STAGE=${DEV_STAGE})

    add_executable(uredis_cluster_example examples/main_cluster.cpp)
    target_link_libraries(uredis_cluster_example PRIVATE uredis)
    target_compile_definitions(uredis_cluster_example PRIVATE DEV_STAGE=${DEV_STAGE})

    add_executable(uredis_redlock_example examples/main_redlock.cpp)
    target_link_libraries(uredis_redlock_example PRIVATE uredis)
    target_compile_definitions(uredis_redlock_example PRIVATE DEV_STAGE=${DEV_STAGE})

    add_executable(uredis_timeout_example examples/timeout/main_timeout.cpp)
    target_link_libraries(uredis_timeout_example PRIVATE uredis)
    target_compile_definitions(uredis_timeout_example PRIVATE DEV_STAGE=${DEV_STAGE})
endif ()

install(TARGETS uredis
        EXPORT uredisTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/uredis
)

install(DIRECTORY include/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/uredis
        FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.hpp"
)

configure_package_config_file(
        cmake/uredisConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/uredisConfig.cmake
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/uredis
        NO_CHECK_REQUIRED_COMPONENTS_MACRO
)

write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/uredisConfigVersion.cmake
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
)

install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/uredisConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/uredisConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/uredis
)

install(EXPORT uredisTargets
        NAMESPACE uredis::
        FILE uredisTargets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/uredis
)

option(UREDIS_BUILD_TESTS "Build uredis tests" ON)
if (BUILD_TESTING AND UREDIS_BUILD_TESTS)
    # GoogleTest
    FetchContent_Declare(
            googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG v1.15.2
            FIND_PACKAGE_ARGS NAMES GTest
    )
    find_package(GTest QUIET)
    if (NOT GTest_FOUND)
        FetchContent_MakeAvailable(googletest)
        set(GTest_LIBS GTest::gtest GTest::gtest_main)
    else ()
        set(GTest_LIBS GTest::gtest GTest::gtest_main)
    endif ()

    file(GLOB_RECURSE UREDIS_TESTS CONFIGURE_DEPENDS tests/*.cpp)

    #    add_executable(urpc_tests ${URPC_TESTS})
    #    target_link_libraries(urpc_tests PRIVATE urpc ${GTest_LIBS})
    #    target_compile_definitions(urpc_tests PRIVATE DEV_STAGE=${DEV_STAGE})
    #    target_compile_features(urpc_tests PRIVATE cxx_std_23)
    #
    #    include(GoogleTest)
    #    gtest_discover_tests(urpc_tests
    #            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    #            DISCOVERY_TIMEOUT 60
    #    )
endif ()