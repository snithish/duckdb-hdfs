include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)

FetchContent_MakeAvailable(googletest)

include(GoogleTest)

enable_testing()

add_executable(
        hdfs_test
        cpp/test_hdfsfs.cpp
)
target_link_libraries(
        hdfs_test
        gtest
        gtest_main
)
gtest_discover_tests(hdfs_test)
