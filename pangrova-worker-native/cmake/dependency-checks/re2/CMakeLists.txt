cmake_minimum_required(VERSION 3.20)
project(PangrovaRe2Contract LANGUAGES CXX)

if(NOT PANGROVA_DEPS_PREFIX)
  message(FATAL_ERROR "PANGROVA_DEPS_PREFIX is required")
endif()
find_package(absl CONFIG REQUIRED
  PATHS "${PANGROVA_DEPS_PREFIX}/lib/cmake/absl" NO_DEFAULT_PATH)
find_package(re2 CONFIG REQUIRED
  PATHS "${PANGROVA_DEPS_PREFIX}/lib/cmake/re2" NO_DEFAULT_PATH)
add_executable(pangrova_re2_contract Re2DependencyContract.cpp)
target_compile_features(pangrova_re2_contract PRIVATE cxx_std_17)
target_compile_options(pangrova_re2_contract PRIVATE -Wall -Wextra -Werror)
target_link_libraries(pangrova_re2_contract PRIVATE re2::re2 absl::flat_hash_set)
set_target_properties(pangrova_re2_contract PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
foreach(configuration DEBUG RELEASE RELWITHDEBINFO MINSIZEREL)
  set_target_properties(pangrova_re2_contract PROPERTIES
    "RUNTIME_OUTPUT_DIRECTORY_${configuration}" "${CMAKE_BINARY_DIR}")
endforeach()
