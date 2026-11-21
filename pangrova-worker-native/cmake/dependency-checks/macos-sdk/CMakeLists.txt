cmake_minimum_required(VERSION 3.20)
project(PangrovaMacosSdkCheck LANGUAGES CXX)

find_package(ZLIB REQUIRED)
add_executable(pangrova_macos_sdk_check SdkHeaderContract.cpp)
target_compile_features(pangrova_macos_sdk_check PRIVATE cxx_std_17)
target_include_directories(pangrova_macos_sdk_check PRIVATE ${ZLIB_INCLUDE_DIRS})
target_link_libraries(pangrova_macos_sdk_check PRIVATE ZLIB::ZLIB)
