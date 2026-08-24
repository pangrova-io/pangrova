# ==============================================================================
# FindLanceCFFI.cmake — Locate Lance C FFI static library (Rust → C ABI)
#
# Lance C FFI is a Rust crate compiled to a C-compatible static library
# (liblance_c_ffi.a). It provides read/write/scan operations for the Lance
# file format, exchanging data via the Arrow C Data Interface.
#
# Input variables (set via -D, env var, or CMakeUserPresets.json):
#   LANCE_FFI_PREFIX  — Install prefix containing lib/liblance_c_ffi.a and
#                       include/lance_c_ffi.h. Defaults to the lance-ffi/target
#                       directory within the source tree for dev builds.
#
# Output:
#   lance_c_ffi           — STATIC IMPORTED target (if found)
#   LANCE_C_FFI_FOUND     — TRUE if the library was found
#   LANCE_C_FFI_LIB       — Path to liblance_c_ffi.a
#   LANCE_C_FFI_INCLUDE   — Path to include directory containing lance_c_ffi.h
# ==============================================================================

# --- Read from environment if not set via -D ---
if(NOT LANCE_FFI_PREFIX AND DEFINED ENV{LANCE_FFI_PREFIX})
  set(LANCE_FFI_PREFIX "$ENV{LANCE_FFI_PREFIX}" CACHE PATH "Lance C FFI install prefix")
endif()

# --- Auto-detect from source tree (dev builds) ---
set(_LANCE_FFI_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/lance-ffi")

if(NOT LANCE_FFI_PREFIX)
  # Determine target triple for the host platform
  if(APPLE)
    set(_LANCE_TARGET "aarch64-apple-darwin")
  elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    set(_LANCE_TARGET "aarch64-unknown-linux-gnu")
  else()
    set(_LANCE_TARGET "x86_64-unknown-linux-gnu")
  endif()

  # Check release build first, then debug
  set(_LANCE_SEARCH_DIRS
    "${_LANCE_FFI_SRC_DIR}/target/${_LANCE_TARGET}/release"
    "${_LANCE_FFI_SRC_DIR}/target/release"
    "${_LANCE_FFI_SRC_DIR}/target/${_LANCE_TARGET}/debug"
    "${_LANCE_FFI_SRC_DIR}/target/debug"
  )

  foreach(_dir ${_LANCE_SEARCH_DIRS})
    if(EXISTS "${_dir}/liblance_c_ffi.a")
      set(LANCE_FFI_PREFIX "${_LANCE_FFI_SRC_DIR}")
      set(_LANCE_C_FFI_LIB_PATH "${_dir}/liblance_c_ffi.a")
      message(STATUS "Lance C FFI: auto-detected from source tree: ${_dir}")
      break()
    endif()
  endforeach()
endif()

# --- Find the library ---
if(NOT _LANCE_C_FFI_LIB_PATH)
  if(LANCE_FFI_PREFIX)
    find_library(_LANCE_C_FFI_LIB_PATH
      NAMES lance_c_ffi
      PATHS "${LANCE_FFI_PREFIX}/lib"
            "${LANCE_FFI_PREFIX}/target/release"
      NO_DEFAULT_PATH
    )
  endif()
endif()

# --- Find the header ---
set(_LANCE_C_FFI_INCLUDE_PATH "")
if(EXISTS "${_LANCE_FFI_SRC_DIR}/include/lance_c_ffi.h")
  set(_LANCE_C_FFI_INCLUDE_PATH "${_LANCE_FFI_SRC_DIR}/include")
elseif(LANCE_FFI_PREFIX AND EXISTS "${LANCE_FFI_PREFIX}/include/lance_c_ffi.h")
  set(_LANCE_C_FFI_INCLUDE_PATH "${LANCE_FFI_PREFIX}/include")
endif()

# --- Create imported target ---
if(_LANCE_C_FFI_LIB_PATH AND _LANCE_C_FFI_INCLUDE_PATH)
  add_library(lance_c_ffi STATIC IMPORTED)
  set_target_properties(lance_c_ffi PROPERTIES
    IMPORTED_LOCATION "${_LANCE_C_FFI_LIB_PATH}"
    INTERFACE_INCLUDE_DIRECTORIES "${_LANCE_C_FFI_INCLUDE_PATH}"
  )

  # Rust static libraries require linking against system libraries
  if(APPLE)
    set_property(TARGET lance_c_ffi APPEND PROPERTY
      INTERFACE_LINK_LIBRARIES "-framework Security" "-framework CoreFoundation" "-lSystem" "-lm")
  else()
    set_property(TARGET lance_c_ffi APPEND PROPERTY
      INTERFACE_LINK_LIBRARIES "-ldl" "-lrt" "-lpthread" "-lm")
  endif()

  set(LANCE_C_FFI_FOUND TRUE)
  set(LANCE_C_FFI_LIB "${_LANCE_C_FFI_LIB_PATH}" CACHE FILEPATH "Lance C FFI static library")
  set(LANCE_C_FFI_INCLUDE "${_LANCE_C_FFI_INCLUDE_PATH}" CACHE PATH "Lance C FFI include directory")
  message(STATUS "Lance C FFI found: ${_LANCE_C_FFI_LIB_PATH}")
  message(STATUS "Lance C FFI include: ${_LANCE_C_FFI_INCLUDE_PATH}")
else()
  set(LANCE_C_FFI_FOUND FALSE)
  message(STATUS "Lance C FFI NOT found — Lance file format support disabled. "
    "Build with: cd pangrova-worker-native/lance-ffi && cargo build --release")
endif()
