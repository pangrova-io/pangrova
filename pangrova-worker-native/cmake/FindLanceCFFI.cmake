# ==============================================================================
# FindLanceCFFI.cmake — Require the pinned Lance Rust C ABI artifact.
#
# There is intentionally one artifact per build:
#   * repository builds consume lance-ffi/target/<host>/release; or
#   * packaged builds set LANCE_FFI_PREFIX to an install tree with lib/include.
#
# Debug archives, Cargo's unqualified target/release directory, and ambient
# system libraries are rejected so a Worker cannot silently link a different
# Lance implementation than the one its headers describe.
# ==============================================================================

set(_LANCE_FFI_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/lance-ffi")

if(APPLE AND (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64" OR
              CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64"))
  set(_LANCE_TARGET "aarch64-apple-darwin")
elseif(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
  set(_LANCE_TARGET "x86_64-apple-darwin")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
       CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
  set(_LANCE_TARGET "x86_64-unknown-linux-gnu")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
       (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64" OR
        CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64"))
  set(_LANCE_TARGET "aarch64-unknown-linux-gnu")
else()
  message(FATAL_ERROR
    "Lance C FFI has no pinned artifact for ${CMAKE_SYSTEM_NAME}/${CMAKE_SYSTEM_PROCESSOR}")
endif()

if(LANCE_FFI_PREFIX)
  set(_LANCE_C_FFI_LIB_PATH
    "${LANCE_FFI_PREFIX}/lib/liblance_c_ffi.a")
  set(_LANCE_C_FFI_INCLUDE_PATH "${LANCE_FFI_PREFIX}/include")
else()
  set(_LANCE_C_FFI_LIB_PATH
    "${_LANCE_FFI_SOURCE_DIR}/target/${_LANCE_TARGET}/release/liblance_c_ffi.a")
  set(_LANCE_C_FFI_INCLUDE_PATH "${_LANCE_FFI_SOURCE_DIR}/include")
endif()

if(NOT EXISTS "${_LANCE_C_FFI_LIB_PATH}")
  message(FATAL_ERROR
    "Required pinned Lance C FFI Release archive is missing: ${_LANCE_C_FFI_LIB_PATH}. "
    "Run pangrova-worker-native/bin/build-lance-ffi.sh before configuring CMake.")
endif()
if(NOT EXISTS "${_LANCE_C_FFI_INCLUDE_PATH}/lance_c_ffi.h")
  message(FATAL_ERROR
    "Required Lance C FFI header is missing: ${_LANCE_C_FFI_INCLUDE_PATH}/lance_c_ffi.h")
endif()

add_library(lance_c_ffi STATIC IMPORTED)
set_target_properties(lance_c_ffi PROPERTIES
  IMPORTED_LOCATION "${_LANCE_C_FFI_LIB_PATH}"
  INTERFACE_INCLUDE_DIRECTORIES "${_LANCE_C_FFI_INCLUDE_PATH}"
)

# Rust static libraries require these platform libraries at the final link.
if(APPLE)
  set_property(TARGET lance_c_ffi APPEND PROPERTY
    INTERFACE_LINK_LIBRARIES
    "-framework Security" "-framework CoreFoundation" "-lSystem" "-lm")
else()
  set_property(TARGET lance_c_ffi APPEND PROPERTY
    INTERFACE_LINK_LIBRARIES "-ldl" "-lrt" "-lpthread" "-lm")
endif()

set(LANCE_C_FFI_LIB "${_LANCE_C_FFI_LIB_PATH}" CACHE FILEPATH
  "Pinned Lance C FFI static library" FORCE)
set(LANCE_C_FFI_INCLUDE "${_LANCE_C_FFI_INCLUDE_PATH}" CACHE PATH
  "Pinned Lance C FFI include directory" FORCE)

message(STATUS "Lance C FFI required artifact: ${LANCE_C_FFI_LIB}")
