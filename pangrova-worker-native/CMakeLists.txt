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

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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
set(_PANGROVA_HOME "$ENV{HOME}/.pangrova")

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

if(PANGROVA_ENABLE_OTEL)
  set(OPENSSL_USE_STATIC_LIBS TRUE)
  if(APPLE AND NOT OPENSSL_ROOT_DIR AND
      EXISTS "${_PANGROVA_HOME}/homebrew/opt/openssl@3/include/openssl/ssl.h")
    set(OPENSSL_ROOT_DIR "${_PANGROVA_HOME}/homebrew/opt/openssl@3")
  endif()
  find_package(OpenSSL REQUIRED)
endif()

find_package(JNI REQUIRED)
find_package(Arrow REQUIRED)
find_package(Parquet QUIET)

include(FindArrowFlight)
include(FindLanceCFFI)
include(FindVelox)
include(PangrovaSystemDeps)
include(PangrovaProtobuf)

if(Parquet_FOUND)
  message(STATUS "Parquet found — format readers will link Parquet")
  add_compile_definitions(PANGROVA_PARQUET_AVAILABLE=1)
else()
  message(STATUS "Parquet NOT found — format readers compile without Parquet link")
  add_compile_definitions(PANGROVA_PARQUET_AVAILABLE=0)
endif()

if(LANCE_C_FFI_FOUND)
  add_compile_definitions(PANGROVA_LANCE_AVAILABLE=1)
else()
  add_compile_definitions(PANGROVA_LANCE_AVAILABLE=0)
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
  # Suppress Clang 17+ error for OpenTelemetry's template keyword usage.
  # Must be set via CMAKE_CXX_FLAGS before FetchContent_MakeAvailable so ALL
  # OTel subdirectory targets (including sdk/src/logs) inherit the flag.
  set(_SAVED_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-template-arg-list-after-template-kw")
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
set(SIMDJSON_JUST_LIBRARY ON CACHE BOOL "" FORCE)
set(SIMDJSON_BUILD_STATIC_LIB ON CACHE BOOL "" FORCE)
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
file(GLOB_RECURSE PANGROVA_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# Exclude Arrow Flight-dependent sources when Flight is not available.
# These files #include <arrow/flight/server.h> or <arrow/flight/client.h> and
# produce undefined symbols that dyld cannot resolve at runtime.
if(NOT ARROW_FLIGHT_FOUND)
  list(FILTER PANGROVA_SOURCES EXCLUDE REGEX "FlightShuffleServer\\.cpp|FlightResultServer\\.cpp|PangrovaExchangeSource\\.cpp|FlightServerRegistry\\.cpp")
  message(STATUS "Arrow Flight not available — excluded FlightShuffleServer, FlightResultServer, PangrovaExchangeSource, FlightServerRegistry")
elseif(ARROW_FLIGHT_HEADERS_ONLY)
  # Headers available for compilation but no library to link.
  # Exclude .cpp files that define Flight server implementations (they reference
  # libarrow_flight symbols), but keep all other files that only #include Flight
  # headers for type definitions.
  list(FILTER PANGROVA_SOURCES EXCLUDE REGEX "FlightShuffleServer\\.cpp|FlightResultServer\\.cpp|PangrovaExchangeSource\\.cpp|FlightServerRegistry\\.cpp")
  add_compile_definitions(PANGROVA_HAS_FLIGHT=0)
  message(STATUS "Arrow Flight headers-only — excluded Flight server implementations, kept type definitions")
else()
  add_compile_definitions(PANGROVA_HAS_FLIGHT=1)
endif()

if(PANGROVA_SOURCES)
  # OBJECT library: compiles sources once, shared by both SHARED lib and tests.
  # This avoids double compilation while allowing different visibility settings.
  add_library(pangrova_native_objects OBJECT ${PANGROVA_SOURCES})
  target_include_directories(pangrova_native_objects PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${JNI_INCLUDE_DIRS}
  )

  # ============================================================================
  # Task 27.3: Strict warnings for first-party (src/) code only.
  # Velox / folly / arrow headers trigger many warnings we cannot fix, so we
  # apply these flags via target_compile_options on our OBJECT library rather
  # than add_compile_options (which would leak into FetchContent deps).
  # ============================================================================
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(pangrova_native_objects PRIVATE
      -Wall
      -Wextra
      -fstack-protector-strong
      # Silence noise from generated protobuf code + Velox public headers
      # included from first-party translation units.
      -Wno-unused-parameter
      -Wno-deprecated-declarations
    )
    if(PANGROVA_WERROR)
      target_compile_options(pangrova_native_objects PRIVATE -Werror)
    endif()
  endif()

  # --- Core dependencies on OBJECT library ---
  target_link_libraries(pangrova_native_objects PUBLIC
    Arrow::arrow_shared
    roaring
    roaring-headers-cpp
    simdjson
  )
  if(ARROW_FLIGHT_FOUND)
    target_link_libraries(pangrova_native_objects PUBLIC arrow_flight_imported)
  endif()
  if(Parquet_FOUND)
    target_link_libraries(pangrova_native_objects PUBLIC Parquet::parquet_shared)
  endif()
  if(TARGET pangrova_proto)
    target_link_libraries(pangrova_native_objects PUBLIC pangrova_proto)
  endif()
  if(PANGROVA_HAS_OTEL)
    target_compile_definitions(pangrova_native_objects PUBLIC HAS_OTEL_CPP=1)
    target_link_libraries(pangrova_native_objects PUBLIC
      opentelemetry_trace
      opentelemetry_exporter_otlp_grpc
      opentelemetry_resources
      opentelemetry_common
    )
  endif()
  if(VELOX_FOUND)
    target_link_libraries(pangrova_native_objects PUBLIC velox_imported)
  endif()
  if(LANCE_C_FFI_FOUND)
    target_link_libraries(pangrova_native_objects PUBLIC lance_c_ffi)
  endif()

  # SHARED library: uses OBJECT library + adds visibility hidden + Velox force-load.
  add_library(pangrova_native SHARED)
  target_include_directories(pangrova_native PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${JNI_INCLUDE_DIRS}
  )
  # Link the OBJECT library — this pulls in both .o files and transitive deps.
  target_link_libraries(pangrova_native PUBLIC pangrova_native_objects)

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
    else()
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
    target_compile_options(pangrova_native PRIVATE -fvisibility=hidden)

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
  get_target_property(_arrow_loc Arrow::arrow_shared IMPORTED_LOCATION)
  if(_arrow_loc)
    get_filename_component(_arrow_dir "${_arrow_loc}" DIRECTORY)
    list(APPEND _RPATH_DIRS "${_arrow_dir}")
  endif()
  # glog/gflags from deps-install
  if(_DEPS_INSTALL AND EXISTS "${_DEPS_INSTALL}/lib")
    list(APPEND _RPATH_DIRS "${_DEPS_INSTALL}/lib")
  endif()
  # Arrow Flight from ARROW_FLIGHT_HOME or system Arrow for Velox vtable resolution.
  # Velox force-load pulls in FlightServerBase vtable refs that dlopen must resolve.
  if(NOT ARROW_FLIGHT_FOUND AND _arrow_loc)
    get_filename_component(_arrow_parent "${_arrow_dir}" DIRECTORY)
    # Check if arrow_flight exists alongside arrow (common install layout)
    find_library(_ARROW_FLIGHT_RPATH_LIB NAMES arrow_flight
      PATHS "${_arrow_dir}" NO_DEFAULT_PATH)
    if(_ARROW_FLIGHT_RPATH_LIB)
      list(APPEND _RPATH_DIRS "${_arrow_dir}")
      # Directly link arrow_flight so dlopen loads it (RPATH alone is insufficient
      # for two-level namespace dynamic_lookup symbols on macOS).
      target_link_libraries(pangrova_native PUBLIC "${_ARROW_FLIGHT_RPATH_LIB}")
      message(STATUS "Arrow Flight linked for Velox vtable resolution: ${_ARROW_FLIGHT_RPATH_LIB}")
    endif()
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
endif()

# ==============================================================================
# Tests
# ==============================================================================
if(PANGROVA_BUILD_TESTS)
  enable_testing()
  include(GoogleTest)

  file(GLOB_RECURSE TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/test/*.cpp")

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
      ${CMAKE_CURRENT_SOURCE_DIR}/src/compat/ExceptionCheckInstantiations.cpp
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
      pangrova_native_objects
    )
    if(VELOX_FOUND AND VELOX_MONO_LIB)
      if(APPLE)
        target_link_options(pangrova_native_tests PRIVATE
          "-Wl,-force_load,${VELOX_MONO_LIB}")
        target_link_libraries(pangrova_native_tests PRIVATE "-lc++abi")
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
  find_package(Python3 COMPONENTS Interpreter QUIET)
  if(Python3_Interpreter_FOUND)
    add_test(
      NAME patch_flight_types
      COMMAND ${Python3_EXECUTABLE}
              ${CMAKE_CURRENT_SOURCE_DIR}/test/PatchFlightTypesTest.py
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
  else()
    message(WARNING "Python3 not found — patch_flight_types test will not run")
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
message(STATUS "  Lance C FFI:       ${LANCE_C_FFI_FOUND}")
message(STATUS "  OTel SDK:          ${PANGROVA_HAS_OTEL}")
message(STATUS "  CMAKE_IGNORE_PATH: ${CMAKE_IGNORE_PATH}")
message(STATUS "  Tests:             ${PANGROVA_BUILD_TESTS}")
message(STATUS "=================================================")
