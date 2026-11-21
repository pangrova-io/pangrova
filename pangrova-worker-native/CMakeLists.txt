# ==============================================================================
# pangrova-worker-native: C++ Native Execution Engine (Velox-based)
#
# Architecture (industry-standard, modeled after Apache Gluten + Netty tcnative):
#   - Single SHARED library output (libpangrova_native.dylib/so) — no static lib
#   - Modularized cmake/ modules for dependency management
#   - CMakePresets.json for dev/CI/release configuration
#   - Symbol isolation: -fvisibility=hidden (macOS) / version-script (Linux)
#   - Dev builds: dynamic deps with BUILD_RPATH
#   - Production builds: all deps statically linked, self-contained
# ==============================================================================
cmake_minimum_required(VERSION 3.20)
project(pangrova-worker-native VERSION 0.1.0 LANGUAGES CXX)
# The canonical artifact is a shared JNI library. Build every embedded
# static dependency (OpenTelemetry, simdjson, …) as position-independent code
# so AArch64 TLS and relocation models stay valid inside the shared object.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# AArch64 Linux links a very large shared object whose text can exceed the
# +/-128 MiB range of R_AARCH64_CALL26 branches. GNU ld cannot extend those
# branches to outline-atomics helpers inside static archives; lld inserts
# range-extension thunks. The version script also names optional JNI entry
# points that may be absent for a given link.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
  find_program(PANGROVA_LLD_LINKER ld.lld)
  if(PANGROVA_LLD_LINKER)
    add_link_options(-fuse-ld=lld "-Wl,--undefined-version")
    message(STATUS "AArch64 Linux linker: lld with range-extension thunks")
  endif()
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(PANGROVA_PRODUCTION_BUILD "Build self-contained Linux production JNI" OFF)
if(PANGROVA_PRODUCTION_BUILD)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "PANGROVA_PRODUCTION_BUILD currently requires Linux")
  endif()
  set(_PANGROVA_SYSTEM_LIBRARY_SUFFIXES "${CMAKE_FIND_LIBRARY_SUFFIXES}")
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
  set(OPENSSL_USE_STATIC_LIBS TRUE)
  set(CMAKE_SKIP_RPATH TRUE)
endif()

if(APPLE)
  add_compile_definitions(_DARWIN_C_SOURCE)
  if(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    # Match Velox's -mcpu=apple-m1+crc so Folly F14 header-only templates
    # use the same CRC32 hash path in both Velox's .o files and ours.
    # Without this, an ODR violation causes F14 rehash assertion failures.
    add_compile_options(-mcpu=apple-m1+crc)
  endif()
endif()

# ==============================================================================
# NDEBUG strategy (matches Apache Gluten practice):
# Non-Debug builds explicitly define NDEBUG to disable assert() and
# Folly F14Table debug assertions that cause SIGABRT with real data.
# ==============================================================================
if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_definitions(-DNDEBUG)
  set(_NDEBUG_STATUS "defined (${CMAKE_BUILD_TYPE} build)")
else()
  set(_NDEBUG_STATUS "NOT defined (Debug build — Folly assertions active)")
endif()
message(STATUS "NDEBUG: ${_NDEBUG_STATUS}")

# ==============================================================================
# L3-1/L3-2: Velox feature flags for APIs not yet available in all versions.
# Enable when upgrading Velox to a version that exposes these APIs:
#   cmake -DPANGROVA_VELOX_HAS_PAGE_INDEX=ON -DPANGROVA_VELOX_HAS_BLOOM_FILTER=ON ..
# ==============================================================================
option(PANGROVA_VELOX_HAS_PAGE_INDEX
    "Velox RowReaderOptions has setColumnReaderStatisticsBasedFiltering()" OFF)
option(PANGROVA_VELOX_HAS_BLOOM_FILTER
    "Velox RowReaderOptions has setUseBloomFilter()" OFF)
if(PANGROVA_VELOX_HAS_PAGE_INDEX)
  add_definitions(-DPANGROVA_VELOX_HAS_PAGE_INDEX)
  message(STATUS "Velox Page Index API: ENABLED")
endif()
if(PANGROVA_VELOX_HAS_BLOOM_FILTER)
  add_definitions(-DPANGROVA_VELOX_HAS_BLOOM_FILTER)
  message(STATUS "Velox Bloom Filter API: ENABLED")
endif()

# ==============================================================================
# Environment auto-detection with smart defaults
#
# Priority: explicit -D flag > environment variable > ~/.pangrova convention
# This eliminates the need to source env.sh for most development workflows.
# ==============================================================================
if(PANGROVA_HOME)
  set(_PANGROVA_HOME "${PANGROVA_HOME}")
elseif(DEFINED ENV{PANGROVA_HOME} AND NOT "$ENV{PANGROVA_HOME}" STREQUAL "")
  set(_PANGROVA_HOME "$ENV{PANGROVA_HOME}")
else()
  set(_PANGROVA_HOME "$ENV{HOME}/.pangrova")
endif()

# PANGROVA_DEPS_PREFIX: hermetic native dependencies
if(NOT PANGROVA_DEPS_PREFIX)
  if(DEFINED ENV{PANGROVA_DEPS_PREFIX})
    set(PANGROVA_DEPS_PREFIX "$ENV{PANGROVA_DEPS_PREFIX}")
  elseif(EXISTS "${_PANGROVA_HOME}/native-deps")
    set(PANGROVA_DEPS_PREFIX "${_PANGROVA_HOME}/native-deps")
    message(STATUS "Auto-detected PANGROVA_DEPS_PREFIX: ${PANGROVA_DEPS_PREFIX}")
  endif()
endif()
if(NOT PANGROVA_DEPS_PREFIX OR NOT EXISTS "${PANGROVA_DEPS_PREFIX}")
  message(FATAL_ERROR
    "PANGROVA_DEPS_PREFIX not set or directory does not exist.\n"
    "Run: ./pangrova-worker-native/bin/setup-native-build-env.sh\n"
    "Or set -DPANGROVA_DEPS_PREFIX=/path/to/native-deps")
endif()

# VELOX_HOME: Velox source/install root
if(NOT VELOX_HOME)
  if(DEFINED ENV{VELOX_HOME})
    set(VELOX_HOME "$ENV{VELOX_HOME}")
  elseif(EXISTS "${_PANGROVA_HOME}/velox")
    set(VELOX_HOME "${_PANGROVA_HOME}/velox")
    message(STATUS "Auto-detected VELOX_HOME: ${VELOX_HOME}")
  endif()
endif()

# VELOX_BUILD_DIR: Velox cmake build directory
if(NOT VELOX_BUILD_DIR)
  if(DEFINED ENV{VELOX_BUILD_DIR})
    set(VELOX_BUILD_DIR "$ENV{VELOX_BUILD_DIR}")
  elseif(VELOX_HOME AND EXISTS "${VELOX_HOME}/_build/release")
    set(VELOX_BUILD_DIR "${VELOX_HOME}/_build/release")
    message(STATUS "Auto-detected VELOX_BUILD_DIR: ${VELOX_BUILD_DIR}")
  endif()
endif()

# VELOX_INSTALL_PREFIX: Velox install prefix (often same as VELOX_HOME)
if(NOT VELOX_INSTALL_PREFIX)
  if(DEFINED ENV{VELOX_INSTALL_PREFIX})
    set(VELOX_INSTALL_PREFIX "$ENV{VELOX_INSTALL_PREFIX}")
  elseif(VELOX_HOME)
    set(VELOX_INSTALL_PREFIX "${VELOX_HOME}")
    message(STATUS "Auto-detected VELOX_INSTALL_PREFIX: ${VELOX_INSTALL_PREFIX}")
  endif()
endif()

# CMAKE_PREFIX_PATH: ensure hermetic deps are found first
if(NOT CMAKE_PREFIX_PATH)
  set(CMAKE_PREFIX_PATH "${PANGROVA_DEPS_PREFIX}")
endif()

# CMAKE_IGNORE_PATH: block system package contamination
if(NOT CMAKE_IGNORE_PATH)
  set(CMAKE_IGNORE_PATH "/opt/local;/opt/homebrew")
  message(STATUS "Auto-set CMAKE_IGNORE_PATH: ${CMAKE_IGNORE_PATH}")
elseif(NOT CMAKE_IGNORE_PATH MATCHES "/opt/local")
  message(WARNING
    "CMAKE_IGNORE_PATH does not include /opt/local — "
    "system protobuf may contaminate the build.")
endif()

# ==============================================================================
# Options
# ==============================================================================
option(PANGROVA_BUILD_TESTS "Build C++ tests" ON)
option(PANGROVA_ENABLE_OTEL "Enable OpenTelemetry C++ SDK for native tracing" ON)
# Warnings-as-errors for first-party code. On by default to catch issues
# early. Developers can disable locally with -DPANGROVA_WERROR=OFF if needed
# during toolchain upgrades.
option(PANGROVA_WERROR "Treat warnings as errors for first-party sources" ON)

# ==============================================================================
# Dependencies (modularized)
# ==============================================================================
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

set(OPENSSL_USE_STATIC_LIBS TRUE)
if(APPLE AND NOT OPENSSL_ROOT_DIR AND
    EXISTS "${_PANGROVA_HOME}/homebrew/opt/openssl@3/include/openssl/ssl.h")
  set(OPENSSL_ROOT_DIR "${_PANGROVA_HOME}/homebrew/opt/openssl@3")
endif()
find_package(OpenSSL REQUIRED)

# Only the JNI headers are consumed. Request the headless JVM component so
# FindJNI does not also require the unrelated AWT library by default.
if(PANGROVA_PRODUCTION_BUILD)
  set(CMAKE_FIND_LIBRARY_SUFFIXES "${_PANGROVA_SYSTEM_LIBRARY_SUFFIXES}")
endif()
find_package(JNI REQUIRED COMPONENTS JVM)
if(PANGROVA_PRODUCTION_BUILD)
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
endif()
find_package(Arrow REQUIRED)
if(NOT ARROW_WITH_LZ4 OR NOT ARROW_WITH_ZSTD)
  message(FATAL_ERROR
    "Canonical Arrow IPC requires LZ4 and ZSTD support; rebuild Arrow with both codecs enabled")
endif()
find_package(Parquet QUIET)
if(PANGROVA_PRODUCTION_BUILD)
  if(NOT TARGET Arrow::arrow_static OR NOT TARGET Parquet::parquet_static)
    message(FATAL_ERROR "Linux production requires static Arrow and Parquet packages")
  endif()
  set(PANGROVA_ARROW_TARGET Arrow::arrow_static)
  set(PANGROVA_PARQUET_TARGET Parquet::parquet_static)
else()
  set(PANGROVA_ARROW_TARGET Arrow::arrow_shared)
  set(PANGROVA_PARQUET_TARGET Parquet::parquet_shared)
endif()

include(FindArrowFlight)
include(FindLanceCFFI)
include(FindVelox)
include(PangrovaSystemDeps)
if(PANGROVA_PRODUCTION_BUILD AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_libraries(velox_imported INTERFACE "-Wl,--end-group")
endif()
include(PangrovaProtobuf)

if(Parquet_FOUND)
  message(STATUS "Parquet found — format readers will link Parquet")
  add_compile_definitions(PANGROVA_PARQUET_AVAILABLE=1)
else()
  message(STATUS "Parquet NOT found — format readers compile without Parquet link")
  add_compile_definitions(PANGROVA_PARQUET_AVAILABLE=0)
endif()

# ==============================================================================
# FetchContent Dependencies
# ==============================================================================
include(FetchContent)

# --- CRoaring (Roaring Bitmaps) ---
if(EXISTS "${_PANGROVA_HOME}/native-deps/src/CRoaring-4.1.7/CMakeLists.txt")
  set(FETCHCONTENT_SOURCE_DIR_ROARING
    "${_PANGROVA_HOME}/native-deps/src/CRoaring-4.1.7")
elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/build/_deps/roaring-src/CMakeLists.txt")
  set(FETCHCONTENT_SOURCE_DIR_ROARING
    "${CMAKE_CURRENT_SOURCE_DIR}/build/_deps/roaring-src")
else()
  unset(FETCHCONTENT_SOURCE_DIR_ROARING CACHE)
endif()
FetchContent_Declare(
  roaring
  GIT_REPOSITORY https://github.com/RoaringBitmap/CRoaring.git
  GIT_TAG        v4.1.7
  GIT_SHALLOW    TRUE
)
set(ROARING_USE_CPM OFF CACHE BOOL "" FORCE)
set(ROARING_BUILD_LTO OFF CACHE BOOL "" FORCE)
set(ENABLE_ROARING_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(roaring)

# --- OpenTelemetry C++ SDK ---
# Requires protobuf + gRPC from PANGROVA_DEPS_PREFIX (hermetic, version-matched).
# No system protobuf/gRPC contamination — CMAKE_PREFIX_PATH ensures find_package
# resolves to the hermetic prefix before any Homebrew/MacPorts/system paths.
if(PANGROVA_ENABLE_OTEL)
  if(EXISTS "${_PANGROVA_HOME}/native-deps/src/opentelemetry-cpp-1.17.0/CMakeLists.txt")
    set(FETCHCONTENT_SOURCE_DIR_OPENTELEMETRY-CPP
        "${_PANGROVA_HOME}/native-deps/src/opentelemetry-cpp-1.17.0")
  else()
    unset(FETCHCONTENT_SOURCE_DIR_OPENTELEMETRY-CPP CACHE)
  endif()
  if(EXISTS "${_PANGROVA_HOME}/native-deps/src/opentelemetry-proto-1.3.2/opentelemetry/proto")
    set(FETCHCONTENT_SOURCE_DIR_OPENTELEMETRY_PROTO
        "${_PANGROVA_HOME}/native-deps/src/opentelemetry-proto-1.3.2")
    set(OTELCPP_PROTO_PATH
        "${_PANGROVA_HOME}/native-deps/src/opentelemetry-proto-1.3.2" CACHE PATH "" FORCE)
  else()
    unset(FETCHCONTENT_SOURCE_DIR_OPENTELEMETRY_PROTO CACHE)
    unset(OTELCPP_PROTO_PATH CACHE)
  endif()

  FetchContent_Declare(
    opentelemetry-cpp
    GIT_REPOSITORY https://github.com/open-telemetry/opentelemetry-cpp.git
    GIT_TAG        v1.17.0
    GIT_SHALLOW    TRUE
    GIT_SUBMODULES ""
  )
  set(WITH_OTLP_GRPC ON CACHE BOOL "" FORCE)
  set(WITH_OTLP_HTTP OFF CACHE BOOL "" FORCE)
  set(WITH_ABSEIL ON CACHE BOOL "" FORCE)
  set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
  set(WITH_EXAMPLES OFF CACHE BOOL "" FORCE)
  set(WITH_BENCHMARK OFF CACHE BOOL "" FORCE)
  set(OTELCPP_MAINTAINER_MODE OFF CACHE BOOL "" FORCE)
  # Suppress the Clang diagnostic only when this compiler implements it.
  # Passing an unknown -Wno-* option emits one warning for every OTel source.
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag(
    "-Wno-missing-template-arg-list-after-template-kw"
    PANGROVA_HAS_MISSING_TEMPLATE_ARG_LIST_SUPPRESSION)
  set(_SAVED_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  if(PANGROVA_HAS_MISSING_TEMPLATE_ARG_LIST_SUPPRESSION)
    set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -Wno-missing-template-arg-list-after-template-kw")
  endif()
  FetchContent_MakeAvailable(opentelemetry-cpp)
  set(CMAKE_CXX_FLAGS "${_SAVED_CXX_FLAGS}")
  set(PANGROVA_HAS_OTEL TRUE)
  message(STATUS "OpenTelemetry C++ SDK enabled (OTLP gRPC)")
else()
  set(PANGROVA_HAS_OTEL FALSE)
  message(STATUS "OpenTelemetry C++ SDK disabled")
endif()

# --- simdjson (required by Velox at runtime — error_codes global) ---
# Velox uses simdjson via FetchContent but libvelox.a leaves error_codes undefined.
# We must compile simdjson ourselves so the symbol is present when JVM dlopen's the dylib.
if(EXISTS "${_PANGROVA_HOME}/native-deps/src/simdjson-4.1.0/CMakeLists.txt")
  set(FETCHCONTENT_SOURCE_DIR_SIMDJSON
      "${_PANGROVA_HOME}/native-deps/src/simdjson-4.1.0")
elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/build/_deps/simdjson-src/CMakeLists.txt")
  set(FETCHCONTENT_SOURCE_DIR_SIMDJSON
      "${CMAKE_CURRENT_SOURCE_DIR}/build/_deps/simdjson-src")
else()
  unset(FETCHCONTENT_SOURCE_DIR_SIMDJSON CACHE)
endif()
FetchContent_Declare(
  simdjson
  GIT_REPOSITORY https://github.com/simdjson/simdjson.git
  GIT_TAG        v4.1.0
  GIT_SHALLOW    TRUE
)
set(SIMDJSON_DEVELOPER_MODE OFF CACHE BOOL "" FORCE)
set(SIMDJSON_BUILD_STATIC_LIB ${PANGROVA_PRODUCTION_BUILD} CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(simdjson)

if(PANGROVA_BUILD_TESTS)
  if(EXISTS "${_PANGROVA_HOME}/native-deps/src/googletest-1.15.2/CMakeLists.txt")
    set(FETCHCONTENT_SOURCE_DIR_GOOGLETEST
        "${_PANGROVA_HOME}/native-deps/src/googletest-1.15.2")
  elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/build/_deps/googletest-src/CMakeLists.txt")
    set(FETCHCONTENT_SOURCE_DIR_GOOGLETEST
        "${CMAKE_CURRENT_SOURCE_DIR}/build/_deps/googletest-src")
  else()
    unset(FETCHCONTENT_SOURCE_DIR_GOOGLETEST CACHE)
  endif()
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.15.2
    GIT_SHALLOW    TRUE
  )
  set(BUILD_GMOCK ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

# ==============================================================================
# Main Library — Single SHARED target (the JNI dylib)
#
# Industry standard (Gluten/DuckDB pattern): only one SHARED library.
# No separate STATIC target — eliminates double compilation.
# C++ tests link directly against this SHARED library.
# ==============================================================================
# Result streaming and remote exchange are mandatory execution dependencies.
add_compile_definitions(PANGROVA_HAS_FLIGHT=1)

# ==============================================================================
# M13a: one SHARED library assembled from responsibility-scoped OBJECT targets.
# Directories do not move in this step; the split proves the include/link DAG
# before M13b relocates sources. The bridge target is the only JNI boundary;
# the execution core does not include AI/index capability implementations.
# ==============================================================================
add_library(pangrova_native_headers INTERFACE)
target_include_directories(pangrova_native_headers INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${JNI_INCLUDE_DIRS}
)

# Aggregates every first-party OBJECT target for the SHARED library and tests.
add_library(pangrova_native_objects INTERFACE)

function(pangrova_add_object_target target)
  file(GLOB_RECURSE _sources CONFIGURE_DEPENDS ${ARGN})
  add_library(${target} OBJECT ${_sources})
  target_link_libraries(${target} PUBLIC
    pangrova_native_headers
    pangrova_native_deps
  )
  target_link_libraries(pangrova_native_objects INTERFACE ${target})

  # Strict warnings for first-party (src/) code only. Velox / folly / arrow
  # headers trigger warnings we cannot fix, so the flags live on the OBJECT
  # targets rather than add_compile_options (which would leak into deps).
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(${target} PRIVATE
      -Wall
      -Wextra
      -fstack-protector-strong
      -Wno-unused-parameter
      -Wno-deprecated-declarations
    )
    if(PANGROVA_WERROR)
      target_compile_options(${target} PRIVATE -Werror)
    endif()
  endif()
  if(APPLE)
    target_compile_options(${target} PRIVATE -fvisibility=hidden)
    set_target_properties(${target} PROPERTIES VISIBILITY_INLINES_HIDDEN YES)
  endif()
endfunction()

pangrova_add_object_target(pangrova_native_command_objects
  ${CMAKE_CURRENT_SOURCE_DIR}/src/command/*.cpp)
pangrova_add_object_target(pangrova_native_jni_objects
  ${CMAKE_CURRENT_SOURCE_DIR}/src/jni/*.cpp)
pangrova_add_object_target(pangrova_native_core_objects
  ${CMAKE_CURRENT_SOURCE_DIR}/src/engine/*.cpp)
pangrova_add_object_target(pangrova_native_capability_objects
  ${CMAKE_CURRENT_SOURCE_DIR}/src/capabilities/*.cpp)
pangrova_add_object_target(pangrova_native_index_objects
  ${CMAKE_CURRENT_SOURCE_DIR}/src/indexing/*.cpp)
pangrova_add_object_target(pangrova_native_vector_objects
  ${CMAKE_CURRENT_SOURCE_DIR}/src/vector/*.cpp)
pangrova_add_object_target(pangrova_native_udf_objects
  ${CMAKE_CURRENT_SOURCE_DIR}/src/udf/*.cpp)
pangrova_add_object_target(pangrova_native_lance_objects
  ${CMAKE_CURRENT_SOURCE_DIR}/src/lance/*.cpp)
pangrova_add_object_target(pangrova_native_spatial_objects
  ${CMAKE_CURRENT_SOURCE_DIR}/src/spatial/*.cpp)

set(PANGROVA_OBJECT_TARGETS
  pangrova_native_command_objects
  pangrova_native_jni_objects
  pangrova_native_core_objects
  pangrova_native_capability_objects
  pangrova_native_index_objects
  pangrova_native_vector_objects
  pangrova_native_udf_objects
  pangrova_native_lance_objects
  pangrova_native_spatial_objects)

# Third-party dependencies are shared by every first-party OBJECT target.
add_library(pangrova_native_deps INTERFACE)
target_link_libraries(pangrova_native_deps INTERFACE
  ${PANGROVA_ARROW_TARGET}
  roaring
  roaring-headers-cpp
  simdjson
  arrow_flight_imported
)
target_link_libraries(pangrova_native_objects INTERFACE pangrova_native_deps)
if(Parquet_FOUND)
  target_link_libraries(pangrova_native_deps INTERFACE ${PANGROVA_PARQUET_TARGET})
endif()
if(TARGET pangrova_proto)
  target_link_libraries(pangrova_native_deps INTERFACE pangrova_proto)
endif()
if(PANGROVA_HAS_OTEL)
  target_compile_definitions(pangrova_native_deps INTERFACE HAS_OTEL_CPP=1)
  target_link_libraries(pangrova_native_deps INTERFACE
    opentelemetry_trace
    opentelemetry_exporter_otlp_grpc
    opentelemetry_resources
    opentelemetry_common
  )
endif()
if(VELOX_FOUND)
  target_link_libraries(pangrova_native_deps INTERFACE velox_imported)
endif()
target_link_libraries(pangrova_native_deps INTERFACE lance_c_ffi)

include(PangrovaFunctionCatalog)

  # SHARED library: uses the OBJECT targets + adds visibility hidden + Velox force-load.
  set(_pangrova_object_files)
  foreach(_pangrova_object_target IN LISTS PANGROVA_OBJECT_TARGETS)
    list(APPEND _pangrova_object_files $<TARGET_OBJECTS:${_pangrova_object_target}>)
  endforeach()
  add_library(pangrova_native SHARED ${_pangrova_object_files})
  target_include_directories(pangrova_native PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${JNI_INCLUDE_DIRS}
  )
  # Object files arrive as sources above; only usage requirements and link
  # libraries are needed here.
  target_link_libraries(pangrova_native PUBLIC pangrova_native_deps)
  if(PANGROVA_PRODUCTION_BUILD)
    target_link_options(pangrova_native PRIVATE "-Wl,-z,defs")
  endif()

  # --- Velox (the sole execution backend) ---
  if(VELOX_FOUND)
    # Force-load libvelox_core.a to resolve circular internal dependencies.
    # Only force-load the mono archive; other archives link normally.
    if(APPLE)
      if(VELOX_MONO_LIB)
        target_link_options(pangrova_native PRIVATE "-Wl,-force_load,${VELOX_MONO_LIB}")
      endif()
      target_link_libraries(pangrova_native PUBLIC "-lc++abi")
      # NO -undefined,dynamic_lookup: the stripped libvelox_core.a + Velox's re2
      # from _deps/ resolves all symbols. Remaining undefined are only system
      # symbols (libc, CoreFoundation, Security, OpenSSL, BZ2) which the system
      # dynamic linker resolves at dlopen time without this flag.
      # The strip script (bin/strip-velox-optional-deps.sh) removes .o files
      # that reference unavailable libraries (DuckDB, geos, S2, tpch, stemmer,
      # protobuf v5 API, Arrow Flight server, Network.framework, BZ2).
    elseif(NOT PANGROVA_PRODUCTION_BUILD)
      if(VELOX_MONO_LIB)
        target_link_options(pangrova_native PRIVATE
          "-Wl,--whole-archive" "${VELOX_MONO_LIB}" "-Wl,--no-whole-archive"
        )
      endif()
    endif()
  endif()

  # ============================================================================
  # Symbol Isolation (industry-standard: Gluten symbols.map / Android NDK)
  # ============================================================================
  if(APPLE)
    # -fvisibility=hidden: all symbols default to hidden in the SHARED library.
    # JNI entry points use JNIEXPORT (__attribute__((visibility("default")))).
    #
    # This is REQUIRED for a clean dylib:
    # - Only JNI entry points (Java_* and JNI_OnLoad) are exported.
    # - All Velox/folly/re2 internal symbols remain hidden, preventing
    #   symbol conflicts with other libraries loaded in the same JVM process.
    # - gflags symbols (fLB::FLAGS_*) from libvelox.a are force-loaded into the
    #   dylib. They're hidden but internally resolvable — Velox code within the
    #   same dylib can reference them without external lookup.
    # Sources are compiled in the OBJECT targets; the visibility flags are
    # applied by pangrova_add_object_target, since setting them only on the
    # source-less SHARED target does not change their visibility.

    # Static Velox/Arrow dependencies are built with default visibility, so a
    # compile flag alone cannot prevent their symbols from being re-exported.
    # Restrict the final Mach-O export trie to JNI entry points.
    set(_JNI_EXPORTED_SYMBOLS_LIST
      "${CMAKE_CURRENT_SOURCE_DIR}/jni_exported_symbols_macos.list")
    if(EXISTS "${_JNI_EXPORTED_SYMBOLS_LIST}")
      target_link_options(pangrova_native PRIVATE
        "-Wl,-exported_symbols_list,${_JNI_EXPORTED_SYMBOLS_LIST}")
      message(STATUS
        "JNI symbol isolation: exported-symbols-list ${_JNI_EXPORTED_SYMBOLS_LIST}")
    else()
      message(FATAL_ERROR
        "JNI exported symbols list not found: ${_JNI_EXPORTED_SYMBOLS_LIST}")
    endif()

    # Suppress linker warnings about "direct access to global weak symbol"
    # caused by visibility mismatch between our hidden .o and Velox's default .a.
    include(CheckLinkerFlag)
    check_linker_flag(CXX "-Wl,-no_warn_direct_access" HAS_NO_WARN_DIRECT_ACCESS)
    if(HAS_NO_WARN_DIRECT_ACCESS)
      target_link_options(pangrova_native PRIVATE "-Wl,-no_warn_direct_access")
    else()
      target_link_options(pangrova_native PRIVATE "-Wl,-w")
    endif()
  else()
    # Linux: version script exports only Java_* and JNI_OnLoad/OnUnload.
    set(_JNI_VERSION_SCRIPT "${CMAKE_CURRENT_SOURCE_DIR}/jni_exported_symbols.map")
    if(EXISTS "${_JNI_VERSION_SCRIPT}")
      target_link_options(pangrova_native PRIVATE
        "-Wl,--version-script=${_JNI_VERSION_SCRIPT}")
      message(STATUS "JNI symbol isolation: version-script ${_JNI_VERSION_SCRIPT}")
    else()
      message(WARNING "JNI version script not found — all symbols exported")
    endif()
  endif()

  # ============================================================================
  # RPATH — Replace install_name_tool hacks with CMake-native mechanism
  #
  # Dev builds: BUILD_RPATH points to dependency library directories so the
  # dylib can find its dynamic deps when loaded from any location (including
  # NativeLibraryLoader's temp directory extraction).
  #
  # Production builds: all deps statically linked, no RPATH needed.
  # ============================================================================
  set(_RPATH_DIRS "")
  if(ARROW_FLIGHT_LIB)
    get_filename_component(_flight_dir "${ARROW_FLIGHT_LIB}" DIRECTORY)
    list(APPEND _RPATH_DIRS "${_flight_dir}")
  endif()
  # Arrow shared lib directory
  get_target_property(_arrow_loc ${PANGROVA_ARROW_TARGET} IMPORTED_LOCATION)
  if(_arrow_loc)
    get_filename_component(_arrow_dir "${_arrow_loc}" DIRECTORY)
    list(APPEND _RPATH_DIRS "${_arrow_dir}")
  endif()
  # glog/gflags from deps-install
  if(_DEPS_INSTALL AND EXISTS "${_DEPS_INSTALL}/lib")
    list(APPEND _RPATH_DIRS "${_DEPS_INSTALL}/lib")
  endif()
  if(APPLE)
    set_target_properties(pangrova_native PROPERTIES
      MACOSX_RPATH TRUE
      BUILD_RPATH "${_RPATH_DIRS}"
      INSTALL_RPATH_USE_LINK_PATH TRUE
    )
  else()
    # Linux: set RPATH so the .so can find dynamic deps at runtime.
    # $ORIGIN allows relative resolution from the .so's own directory.
    list(PREPEND _RPATH_DIRS "$ORIGIN")
    set_target_properties(pangrova_native PROPERTIES
      BUILD_RPATH "${_RPATH_DIRS}"
      INSTALL_RPATH "${_RPATH_DIRS}"
      INSTALL_RPATH_USE_LINK_PATH TRUE
    )
  endif()

  set_target_properties(pangrova_native PROPERTIES
    OUTPUT_NAME "pangrova_native"
    PREFIX "lib"
  )

# ==============================================================================
# Tests
# ==============================================================================
if(PANGROVA_BUILD_TESTS)
  enable_testing()
  include(GoogleTest)

  # Real SDK prerequisite for Java PG publication tests. It loads no Worker/JNI
  # classes and does not link the engine, so test-compile -> SDK tool -> CTest
  # has no Java integration-test/native-test dependency cycle.
  if(NOT TARGET pangrova_proto)
    message(FATAL_ERROR "Managed Lance SDK fixture requires canonical pangrova_proto")
  endif()
  add_executable(pangrova_lance_fixture_tool
    "${CMAKE_CURRENT_SOURCE_DIR}/test-tools/LanceManagedSourceFixtureTool.cpp")
  target_link_libraries(pangrova_lance_fixture_tool PRIVATE
    lance_c_ffi ${PANGROVA_ARROW_TARGET} pangrova_proto)
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(pangrova_lance_fixture_tool PRIVATE
      -Wall -Wextra -fstack-protector-strong -Wno-unused-parameter -Wno-deprecated-declarations)
    if(PANGROVA_WERROR)
      target_compile_options(pangrova_lance_fixture_tool PRIVATE -Werror)
    endif()
  endif()

  file(GLOB_RECURSE TEST_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/test/*.cpp")

  # Velox test utilities (VectorMaker) not in libvelox.a — compile directly.
  set(VELOX_TEST_UTILS "")
  if(EXISTS "${VELOX_INCLUDE_DIR}/velox/vector/tests/utils/VectorMaker.cpp")
    list(APPEND VELOX_TEST_UTILS "${VELOX_INCLUDE_DIR}/velox/vector/tests/utils/VectorMaker.cpp")
  elseif(VELOX_HOME AND EXISTS "${VELOX_HOME}/velox/vector/tests/utils/VectorMaker.cpp")
    list(APPEND VELOX_TEST_UTILS "${VELOX_HOME}/velox/vector/tests/utils/VectorMaker.cpp")
  endif()

  if(TEST_SOURCES)
    # Tests link against the SHARED library (libpangrova_native.dylib) instead of
    # force-loading libvelox.a directly. This is the industry-standard approach
    # (DuckDB, Gluten): the dylib is fully self-contained (no unresolved non-system
    # symbols), so the test executable simply links against it.
    add_executable(pangrova_native_tests ${TEST_SOURCES} ${VELOX_TEST_UTILS}
      # Explicit template instantiations lost during ar x (filename collision in
      # strip-velox-optional-deps.sh). Compiled directly into test binary since
      # the dylib uses -fvisibility=hidden and cannot export these symbols.
      ${CMAKE_CURRENT_SOURCE_DIR}/src/engine/compat/ExceptionCheckInstantiations.cpp
    )
    target_include_directories(pangrova_native_tests PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/src
      ${CMAKE_CURRENT_SOURCE_DIR}/test
    )
    # Keep tests and Velox in one Mach-O image. Velox RTTI is hidden, so linking
    # tests through the dylib makes header-level dynamic_cast calls compare
    # against a second, executable-local RTTI instance on macOS.
    target_link_libraries(pangrova_native_tests PRIVATE
      GTest::gtest
      GTest::gmock
    )
    foreach(_pangrova_object_target IN LISTS PANGROVA_OBJECT_TARGETS)
      target_link_libraries(pangrova_native_tests PRIVATE ${_pangrova_object_target})
    endforeach()
    target_link_libraries(pangrova_native_tests PRIVATE pangrova_native_deps)
    if(VELOX_FOUND AND VELOX_MONO_LIB)
      if(APPLE)
        target_link_options(pangrova_native_tests PRIVATE
          "-Wl,-force_load,${VELOX_MONO_LIB}")
        target_link_libraries(pangrova_native_tests PRIVATE "-lc++abi")

        # Arrow Flight is a shared library that carries its own private gRPC and
        # Abseil implementation.  The ASan test executable also contains an
        # instrumented static Abseil through Velox/Protobuf.  Darwin coalesces
        # weak C++ symbols across Mach-O images, so exporting the executable's
        # Abseil templates makes uninstrumented Flight call into instrumented
        # code with incompatible sanitizer annotations (reported as
        # use-after-poison in absl::Cord).  Keep test-only static dependency
        # symbols private; the executable has no plugin ABI and only main needs
        # to be externally visible.
        if(CMAKE_CXX_FLAGS MATCHES "-fsanitize=address")
          set(_TEST_ASAN_EXPORTED_SYMBOLS_LIST
            "${CMAKE_CURRENT_SOURCE_DIR}/test_asan_exported_symbols_macos.list")
          if(EXISTS "${_TEST_ASAN_EXPORTED_SYMBOLS_LIST}")
            target_link_options(pangrova_native_tests PRIVATE
              "-Wl,-exported_symbols_list,${_TEST_ASAN_EXPORTED_SYMBOLS_LIST}")
          else()
            message(FATAL_ERROR
              "ASan test exported symbols list not found: "
              "${_TEST_ASAN_EXPORTED_SYMBOLS_LIST}")
          endif()
        endif()
      else()
        target_link_options(pangrova_native_tests PRIVATE
          "-Wl,--whole-archive" "${VELOX_MONO_LIB}" "-Wl,--no-whole-archive"
        )
      endif()
    endif()
    if(TARGET pangrova_proto)
      target_link_libraries(pangrova_native_tests PRIVATE pangrova_proto)
    endif()
    if(TARGET pangrova_proto)
      target_link_libraries(pangrova_native_tests PRIVATE pangrova_proto)
    endif()

    gtest_discover_tests(pangrova_native_tests
      DISCOVERY_MODE PRE_TEST
      DISCOVERY_TIMEOUT 120
    )
  endif()

  # patch_flight_types.py rewrites an Arrow header during dependency setup; a
  # silent regression there breaks the build long before any C++ test runs, so
  # its unit tests belong in the same `ctest` invocation as everything else.
  find_package(Python3 3.10 COMPONENTS Interpreter REQUIRED)
  if(Python3_Interpreter_FOUND)
    add_test(
      NAME patch_flight_types
      COMMAND ${Python3_EXECUTABLE}
              ${CMAKE_CURRENT_SOURCE_DIR}/test/PatchFlightTypesTest.py
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_test(
      NAME native_test_fixture_lifecycle
      COMMAND ${Python3_EXECUTABLE} -B
              ${CMAKE_CURRENT_SOURCE_DIR}/test/RunNativeTestsTest.py
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_test(
      NAME canonical_function_provenance
      COMMAND ${Python3_EXECUTABLE}
              ${CMAKE_CURRENT_SOURCE_DIR}/test/CanonicalFunctionProvenanceTest.py
              --cmake ${CMAKE_COMMAND}
              --python ${Python3_EXECUTABLE}
              --source ${VELOX_HOME}
              --include ${VELOX_INCLUDE_DIR}
              --archive ${_VELOX_MONO_LIB}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_test(
      NAME canonical_abseil_package
      COMMAND ${Python3_EXECUTABLE}
              ${CMAKE_CURRENT_SOURCE_DIR}/test/CanonicalAbseilPackageTest.py
              --cmake ${CMAKE_COMMAND}
              --package ${absl_DIR}
              --sdk-deps ${PANGROVA_DEPS_PREFIX}
              --compiler ${CMAKE_CXX_COMPILER}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_test(
      NAME canonical_flight_package
      COMMAND ${Python3_EXECUTABLE}
              ${CMAKE_CURRENT_SOURCE_DIR}/test/CanonicalFlightPackageTest.py
              --cmake ${CMAKE_COMMAND}
              --deps-prefix ${PANGROVA_DEPS_PREFIX}
              --flight-prefix ${ARROW_FLIGHT_HOME}
              --compiler ${CMAKE_CXX_COMPILER}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_test(
      NAME canonical_re2_package
      COMMAND ${Python3_EXECUTABLE}
              ${CMAKE_CURRENT_SOURCE_DIR}/test/CanonicalRe2PackageTest.py
              --cmake ${CMAKE_COMMAND}
              --deps-prefix ${PANGROVA_DEPS_PREFIX}
              --absl-package ${absl_DIR}
              "--cxx-flags=${CMAKE_CXX_FLAGS}"
              "--linker-flags=${CMAKE_EXE_LINKER_FLAGS}"
              --compiler ${CMAKE_CXX_COMPILER}
              --ar ${CMAKE_AR}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
  endif()
endif()

# ==============================================================================
# Build Diagnostic Summary
# ==============================================================================
message(STATUS "")
message(STATUS "========== Pangrova Native Build Summary ==========")
message(STATUS "  Build type:        ${CMAKE_BUILD_TYPE}")
message(STATUS "  NDEBUG:            ${_NDEBUG_STATUS}")
message(STATUS "  protoc:            ${PROTOC_EXECUTABLE}")
message(STATUS "  libprotobuf:       ${_PB_SOURCE}")
message(STATUS "  Velox:             ${VELOX_HOME}")
message(STATUS "  Lance C FFI:       ${LANCE_C_FFI_LIB}")
message(STATUS "  OTel SDK:          ${PANGROVA_HAS_OTEL}")
message(STATUS "  CMAKE_IGNORE_PATH: ${CMAKE_IGNORE_PATH}")
message(STATUS "  Tests:             ${PANGROVA_BUILD_TESTS}")
message(STATUS "=================================================")
