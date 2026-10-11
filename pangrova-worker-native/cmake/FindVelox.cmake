# ==============================================================================
# FindVelox.cmake — Locate Velox installation and create velox_imported target
#
# Input variables (REQUIRED — set via -D, env var, or CMakeUserPresets.json):
#   VELOX_HOME            — Velox source root (contains velox/core/PlanNode.h)
#   VELOX_BUILD_DIR       — Velox cmake build directory (contains _deps/)
#   VELOX_INSTALL_PREFIX  — Velox install prefix (contains lib/velox/libvelox.a)
#
# At least VELOX_INSTALL_PREFIX or VELOX_HOME must be set. No hardcoded path
# guessing — all paths are explicit. Use CMakeUserPresets.json for local dev.
#
# Output:
#   velox_imported        — INTERFACE target with all Velox headers + libraries
#   VELOX_FOUND           — TRUE if Velox was found
#   VELOX_MONO_LIB        — Path to libvelox.a (for -force_load / --whole-archive)
#   VELOX_INCLUDE_DIR     — Path to Velox headers
#   _DEPS_INSTALL         — Path to deps-install/ (for glog/gflags resolution)
# ==============================================================================

# Read from environment if not set via -D
if(NOT VELOX_HOME AND DEFINED ENV{VELOX_HOME})
  set(VELOX_HOME "$ENV{VELOX_HOME}" CACHE PATH "Velox source or install root")
endif()
if(NOT VELOX_BUILD_DIR AND DEFINED ENV{VELOX_BUILD_DIR})
  set(VELOX_BUILD_DIR "$ENV{VELOX_BUILD_DIR}" CACHE PATH "Velox cmake build dir")
endif()
if(NOT VELOX_INSTALL_PREFIX AND DEFINED ENV{VELOX_INSTALL_PREFIX})
  set(VELOX_INSTALL_PREFIX "$ENV{VELOX_INSTALL_PREFIX}" CACHE PATH "Velox install prefix")
endif()

if(NOT VELOX_HOME AND NOT VELOX_INSTALL_PREFIX)
  message(FATAL_ERROR
    "Velox NOT found. Set at least one of:\n"
    "  -DVELOX_INSTALL_PREFIX=/path/to/velox-install\n"
    "  -DVELOX_HOME=/path/to/velox-src\n"
    "  or set VELOX_INSTALL_PREFIX / VELOX_HOME environment variables\n"
    "  or create CMakeUserPresets.json with cacheVariables (see CMakePresets.json)")
endif()

# --- System protobuf contamination guard ---
if(EXISTS "/opt/local/include/google/protobuf/" AND NOT CMAKE_IGNORE_PATH MATCHES "/opt/local")
  message(FATAL_ERROR
    "System protobuf detected at /opt/local — "
    "set CMAKE_IGNORE_PATH or source ~/.pangrova/env.sh")
endif()

# --- Determine include dir ---
if(VELOX_INSTALL_PREFIX AND EXISTS "${VELOX_INSTALL_PREFIX}/include/velox/core/PlanNode.h")
  set(VELOX_INCLUDE_DIR "${VELOX_INSTALL_PREFIX}/include")
elseif(VELOX_HOME AND EXISTS "${VELOX_HOME}/include/velox/core/PlanNode.h")
  set(VELOX_INCLUDE_DIR "${VELOX_HOME}/include")
elseif(VELOX_HOME AND EXISTS "${VELOX_HOME}/velox/core/PlanNode.h")
  set(VELOX_INCLUDE_DIR "${VELOX_HOME}")
else()
  message(FATAL_ERROR
    "Velox headers not found. Checked:\n"
    "  ${VELOX_INSTALL_PREFIX}/include/velox/core/PlanNode.h\n"
    "  ${VELOX_HOME}/include/velox/core/PlanNode.h\n"
    "  ${VELOX_HOME}/velox/core/PlanNode.h")
endif()

# --- Determine build dir (for vendored _deps) ---
# If not explicitly set, try a conventional subdirectory of VELOX_HOME.
if(NOT VELOX_BUILD_DIR OR NOT EXISTS "${VELOX_BUILD_DIR}")
  if(VELOX_HOME AND EXISTS "${VELOX_HOME}/_build/release/_deps")
    set(VELOX_BUILD_DIR "${VELOX_HOME}/_build/release" CACHE PATH "" FORCE)
  endif()
endif()

message(STATUS "Velox home:           ${VELOX_HOME}")
message(STATUS "Velox install prefix: ${VELOX_INSTALL_PREFIX}")
message(STATUS "Velox includes:       ${VELOX_INCLUDE_DIR}")
message(STATUS "Velox build dir:      ${VELOX_BUILD_DIR}")

# --- Create velox_imported INTERFACE target ---
add_library(velox_imported INTERFACE)
include("${CMAKE_CURRENT_LIST_DIR}/PangrovaAbseil.cmake")

# 1) Velox headers
target_include_directories(velox_imported SYSTEM INTERFACE "${VELOX_INCLUDE_DIR}")
if(VELOX_HOME AND NOT "${VELOX_HOME}" STREQUAL "${VELOX_INCLUDE_DIR}")
  target_include_directories(velox_imported SYSTEM INTERFACE "${VELOX_HOME}")
endif()

# 2) Velox mono library (libvelox.a)
set(_VELOX_MONO_LIB "")
# Sanitizer builds must link the matching instrumented Velox archive.  Using
# the default Release archive can report false container overflows (and leaves
# the dependency's own memory accesses invisible to the sanitizer).
if(CMAKE_CXX_FLAGS MATCHES "-fsanitize=address")
  if(NOT VELOX_BUILD_DIR
      OR NOT EXISTS "${VELOX_BUILD_DIR}/lib/libvelox.a"
      OR NOT EXISTS "${VELOX_BUILD_DIR}/CMakeCache.txt")
    message(FATAL_ERROR
      "ASan requires an instrumented Velox build in VELOX_BUILD_DIR")
  endif()
  file(STRINGS "${VELOX_BUILD_DIR}/CMakeCache.txt" _VELOX_CXX_FLAGS
    REGEX "^CMAKE_CXX_FLAGS:.*=")
  if(NOT _VELOX_CXX_FLAGS MATCHES "-fsanitize=address")
    message(FATAL_ERROR
      "ASan cannot link an uninstrumented Velox archive: ${VELOX_BUILD_DIR}")
  endif()
  set(_VELOX_MONO_LIB "${VELOX_BUILD_DIR}/lib/libvelox.a")
  set(VELOX_MONO_STRIPPED FALSE)
endif()
# Prefer libvelox_core.a (stripped of optional modules with unavailable external deps)
# over the full libvelox.a. The core version eliminates the need for
# -undefined,dynamic_lookup since all referenced symbols are resolvable.
set(_PANGROVA_DEPS_INSTALL "${CMAKE_CURRENT_SOURCE_DIR}/../deps-install")
if(NOT _VELOX_MONO_LIB AND EXISTS "${_PANGROVA_DEPS_INSTALL}/lib/libvelox_core.a")
  set(_VELOX_MONO_LIB "${_PANGROVA_DEPS_INSTALL}/lib/libvelox_core.a")
  set(VELOX_MONO_STRIPPED TRUE)
endif()
if(NOT _VELOX_MONO_LIB AND VELOX_BUILD_DIR
    AND EXISTS "${VELOX_BUILD_DIR}/lib/libvelox.a")
  set(_VELOX_MONO_LIB "${VELOX_BUILD_DIR}/lib/libvelox.a")
  set(VELOX_MONO_STRIPPED FALSE)
endif()
if(NOT _VELOX_MONO_LIB)
  foreach(_prefix ${VELOX_INSTALL_PREFIX} ${VELOX_HOME})
    if(_prefix AND NOT _VELOX_MONO_LIB)
      foreach(_subdir "lib/velox" "lib" "_build/release/lib")
        if(EXISTS "${_prefix}/${_subdir}/libvelox.a")
          set(_VELOX_MONO_LIB "${_prefix}/${_subdir}/libvelox.a")
          set(VELOX_MONO_STRIPPED FALSE)
          break()
        endif()
      endforeach()
    endif()
  endforeach()
endif()
if(_VELOX_MONO_LIB)
  include("${CMAKE_CURRENT_LIST_DIR}/PangrovaFunctionProvenance.cmake")
  pangrova_verify_function_provenance("${VELOX_HOME}" "${VELOX_INCLUDE_DIR}" "${_VELOX_MONO_LIB}")
  message(STATUS "Velox mono lib: ${_VELOX_MONO_LIB} (stripped=${VELOX_MONO_STRIPPED})")
  set(VELOX_MONO_LIB "${_VELOX_MONO_LIB}" CACHE FILEPATH "" FORCE)
  set(VELOX_MONO_STRIPPED "${VELOX_MONO_STRIPPED}" CACHE BOOL "" FORCE)
  # NOTE: Do NOT add libvelox.a to velox_imported INTERFACE_LINK_LIBRARIES.
  # The mono lib is linked via -force_load (macOS) or --whole-archive (Linux)
  # in CMakeLists.txt. Adding it here would cause it to be linked TWICE:
  # once via force-load (stripped for tests) and once via normal link (full),
  # defeating the S3 .o removal for test binaries.
else()
  message(WARNING "libvelox.a NOT found — Velox symbols will be MISSING.")
endif()

# 3a) deps-install (folly, glog, gflags, fmt, boost, etc.)
# Search relative to VELOX_HOME and VELOX_INSTALL_PREFIX only — no hardcoded paths.
set(_DEPS_INSTALL "")
foreach(_di "${VELOX_HOME}/deps-install" "${VELOX_INSTALL_PREFIX}/deps-install")
  if(_di AND EXISTS "${_di}/include/folly/Executor.h")
    set(_DEPS_INSTALL "${_di}")
    break()
  endif()
endforeach()

if(_DEPS_INSTALL)
  message(STATUS "Velox deps-install: ${_DEPS_INSTALL}")
  target_include_directories(velox_imported SYSTEM INTERFACE "${_DEPS_INSTALL}/include")
  if(APPLE AND EXISTS "$ENV{HOME}/.pangrova/homebrew/include/event.h")
    target_include_directories(velox_imported SYSTEM INTERFACE
      "$ENV{HOME}/.pangrova/homebrew/include")
    foreach(_velox_system_lib zstd snappy event lzma lz4 bz2)
      if(EXISTS "$ENV{HOME}/.pangrova/homebrew/lib/lib${_velox_system_lib}.a")
        target_link_libraries(velox_imported INTERFACE
          "$ENV{HOME}/.pangrova/homebrew/lib/lib${_velox_system_lib}.a")
      endif()
    endforeach()
  endif()
  file(GLOB _DEPS_INSTALL_LIBS "${_DEPS_INSTALL}/lib/*.a")
  if(_DEPS_INSTALL_LIBS)
    # Exclude libgflags*.a — glog links gflags dynamically on macOS.
    # Including static gflags causes dual-registration of global flags → fatal error.
    list(FILTER _DEPS_INSTALL_LIBS EXCLUDE REGEX "libgflags")
    # RE2's complete canonical package owns both headers and library. A different
    # deps-install archive must not replace it or supply duplicate definitions.
    list(FILTER _DEPS_INSTALL_LIBS EXCLUDE REGEX "libre2")
    pangrova_select_abseil_targets(_DEPS_INSTALL_LIBS ${_DEPS_INSTALL_LIBS})
    target_link_libraries(velox_imported INTERFACE ${_DEPS_INSTALL_LIBS})
    include("${CMAKE_CURRENT_LIST_DIR}/PangrovaRe2.cmake")
    target_link_libraries(velox_imported INTERFACE re2::re2)
    message(STATUS "re2 (canonical package): ${_PANGROVA_RE2_DIR}")
    list(LENGTH _DEPS_INSTALL_LIBS _N_DI)
    message(STATUS "Velox deps-install libs: ${_N_DI} .a files (gflags, re2 excluded)")
  endif()
else()
  message(WARNING "Velox deps-install NOT found — folly/glog/gflags headers may be missing")
endif()

# 3b) FetchContent deps from the build tree (_deps/)
if(VELOX_BUILD_DIR AND EXISTS "${VELOX_BUILD_DIR}/_deps")
  set(_DEPS "${VELOX_BUILD_DIR}/_deps")
  set(_VENDORED_INCLUDE_DIRS)

  # Vendored header directories
  # NOTE: protobuf-src and absl-src are EXCLUDED because Velox now uses
  # the hermetic protobuf v4.25.3 + abseil from PANGROVA_DEPS_PREFIX
  # (Velox was rebuilt with CMAKE_PREFIX_PATH pointing to native-deps).
  # The hermetic headers are provided via bundled_protobuf target.
  foreach(_pair
    "folly-src;."
    "folly-build;."
    "glog-src;src"
    "glog-build;."
    "gflags-build;include"
    "fmt-src;include"
  )
    string(REPLACE ";" ";" _parts "${_pair}")
    list(GET _parts 0 _pkg)
    list(GET _parts 1 _sub)
    if(_sub STREQUAL ".")
      set(_check_dir "${_DEPS}/${_pkg}")
    else()
      set(_check_dir "${_DEPS}/${_pkg}/${_sub}")
    endif()
    if(EXISTS "${_check_dir}")
      list(APPEND _VENDORED_INCLUDE_DIRS "${_check_dir}")
    endif()
  endforeach()

  foreach(_hdr_pkg xsimd simdjson fastfloat)
    if(EXISTS "${_DEPS}/${_hdr_pkg}-src/include")
      list(APPEND _VENDORED_INCLUDE_DIRS "${_DEPS}/${_hdr_pkg}-src/include")
    endif()
  endforeach()

  # xsimd may be installed to a separate prefix (not in _deps/).
  # Check Velox's CMakeCache for xsimd_DIR and derive the include path.
  if(NOT EXISTS "${_DEPS}/xsimd-src/include" AND VELOX_BUILD_DIR
      AND EXISTS "${VELOX_BUILD_DIR}/CMakeCache.txt")
    file(STRINGS "${VELOX_BUILD_DIR}/CMakeCache.txt" _XSIMD_LINES
      REGEX "^xsimd_DIR:PATH=")
    list(LENGTH _XSIMD_LINES _XSIMD_COUNT)
    if(_XSIMD_COUNT GREATER 0)
      list(GET _XSIMD_LINES 0 _XSIMD_LINE)
      string(REGEX REPLACE "^xsimd_DIR:PATH=" "" _XSIMD_CMAKE_DIR "${_XSIMD_LINE}")
      # xsimd_DIR points to <prefix>/lib/cmake/xsimd — include is at <prefix>/include
      get_filename_component(_XSIMD_PREFIX "${_XSIMD_CMAKE_DIR}" DIRECTORY)  # lib/cmake
      get_filename_component(_XSIMD_PREFIX "${_XSIMD_PREFIX}" DIRECTORY)     # lib
      get_filename_component(_XSIMD_PREFIX "${_XSIMD_PREFIX}" DIRECTORY)     # <prefix>
      if(EXISTS "${_XSIMD_PREFIX}/include/xsimd")
        list(APPEND _VENDORED_INCLUDE_DIRS "${_XSIMD_PREFIX}/include")
        message(STATUS "xsimd headers (from Velox CMakeCache): ${_XSIMD_PREFIX}/include")
      endif()
    endif()
  endif()

  if(EXISTS "${_DEPS}/boost-src/libs")
    file(GLOB _BOOST_MODULES "${_DEPS}/boost-src/libs/*/include")
    list(APPEND _VENDORED_INCLUDE_DIRS ${_BOOST_MODULES})
  endif()

  if(EXISTS "${_DEPS}/libstemmer/src/libstemmer/include")
    list(APPEND _VENDORED_INCLUDE_DIRS "${_DEPS}/libstemmer/src/libstemmer/include")
  endif()

  target_include_directories(velox_imported INTERFACE ${_VENDORED_INCLUDE_DIRS})

  file(GLOB_RECURSE _VENDORED_LIBS "${_DEPS}/*.a")
  if(_VENDORED_LIBS)
    list(FILTER _VENDORED_LIBS EXCLUDE REGEX "(test|Test|benchmark|Benchmark|example|Example|_test\\.|_bench\\.)")
    # Exclude protobuf libs — we use our own hermetic protobuf via bundled_protobuf
    # target (PangrovaProtobuf.cmake). Including Velox's bundled protobuf causes
    # duplicate symbol errors at link time.
    # Instead, Velox's protobuf is linked separately AFTER the hermetic one
    # (see CMakeLists.txt VELOX_PROTOBUF_SUPPLEMENT) to provide symbols like
    # CopyWithSourceCheck that exist in protobuf v5 but not in hermetic v4.
    list(FILTER _VENDORED_LIBS EXCLUDE REGEX "protobuf-build")
    # Exclude c-ares libs — we use our own hermetic c-ares from PANGROVA_DEPS_PREFIX.
    # Including Velox's bundled c-ares causes duplicate symbol errors (ares_inet_pton).
    list(FILTER _VENDORED_LIBS EXCLUDE REGEX "c-ares-build")
    list(FILTER _VENDORED_LIBS EXCLUDE REGEX "libre2\\.a$")
    pangrova_select_abseil_targets(_VENDORED_LIBS ${_VENDORED_LIBS})
    target_link_libraries(velox_imported INTERFACE ${_VENDORED_LIBS})

    # Velox uses SYSTEM protobuf (hermetic v4.25.3 from PANGROVA_DEPS_PREFIX).
    # libvelox.a references protobuf symbols but doesn't bundle them.
    # Link hermetic libprotobuf to resolve these symbols.
    if(PANGROVA_DEPS_PREFIX AND EXISTS "${PANGROVA_DEPS_PREFIX}/lib/libprotobuf.a")
      set(_VELOX_PROTOBUF_LIB "${PANGROVA_DEPS_PREFIX}/lib/libprotobuf.a")
      if(CMAKE_CXX_FLAGS MATCHES "-fsanitize=address")
        if(NOT PANGROVA_ASAN_PROTOBUF_LIB
            OR NOT EXISTS "${PANGROVA_ASAN_PROTOBUF_LIB}")
          message(FATAL_ERROR
            "ASan requires an instrumented v25.3 libprotobuf.a in "
            "PANGROVA_ASAN_PROTOBUF_LIB")
        endif()
        set(_VELOX_PROTOBUF_LIB "${PANGROVA_ASAN_PROTOBUF_LIB}")
      endif()
      target_link_libraries(velox_imported INTERFACE "${_VELOX_PROTOBUF_LIB}")
      message(STATUS "Velox protobuf (hermetic): ${_VELOX_PROTOBUF_LIB}")
    endif()
    list(LENGTH _VENDORED_LIBS _N_VENDORED)
    message(STATUS "Velox vendored libs: ${_N_VENDORED} .a files from _deps/")
  endif()
endif()

# 5) Additional libraries from VELOX_INSTALL_PREFIX/lib/
if(VELOX_INSTALL_PREFIX AND EXISTS "${VELOX_INSTALL_PREFIX}/lib")
  file(GLOB _INSTALL_PREFIX_LIBS "${VELOX_INSTALL_PREFIX}/lib/*.a")
  if(_INSTALL_PREFIX_LIBS)
    list(FILTER _INSTALL_PREFIX_LIBS EXCLUDE REGEX "libvelox\\.a$")
    list(FILTER _INSTALL_PREFIX_LIBS EXCLUDE REGEX "libre2\\.a$")
    pangrova_select_abseil_targets(_INSTALL_PREFIX_LIBS ${_INSTALL_PREFIX_LIBS})
    if(_INSTALL_PREFIX_LIBS)
      target_link_libraries(velox_imported INTERFACE ${_INSTALL_PREFIX_LIBS})
      list(LENGTH _INSTALL_PREFIX_LIBS _N_IP)
      message(STATUS "Velox install-prefix libs: ${_N_IP} .a files")
    endif()
  endif()
endif()

# 6) glog + gflags (prefer shared to avoid dual-registration of gflags globals)
if(_DEPS_INSTALL AND PANGROVA_PRODUCTION_BUILD)
  find_library(_PRODUCTION_GFLAGS NAMES gflags PATHS "${_DEPS_INSTALL}/lib" REQUIRED)
  target_link_libraries(velox_imported INTERFACE "${_PRODUCTION_GFLAGS}")
endif()

if(_DEPS_INSTALL AND NOT PANGROVA_PRODUCTION_BUILD)
  foreach(_glog_candidate
    "${_DEPS_INSTALL}/lib/libglog.dylib"
    "${_DEPS_INSTALL}/lib/libglog.so"
  )
    if(EXISTS "${_glog_candidate}")
      target_link_libraries(velox_imported INTERFACE "${_glog_candidate}")
      message(STATUS "glog (shared): ${_glog_candidate}")
      break()
    endif()
  endforeach()
  foreach(_gflags_candidate
    "${_DEPS_INSTALL}/lib/libgflags.dylib"
    "${_DEPS_INSTALL}/lib/libgflags.so"
  )
    if(EXISTS "${_gflags_candidate}")
      target_link_libraries(velox_imported INTERFACE "${_gflags_candidate}")
      message(STATUS "gflags (shared): ${_gflags_candidate}")
      break()
    endif()
  endforeach()
endif()

# Linux fallback: if deps-install didn't have glog/gflags, try system
if(NOT APPLE)
  get_target_property(_velox_libs velox_imported INTERFACE_LINK_LIBRARIES)
  string(FIND "${_velox_libs}" "glog" _glog_linked)
  if(_glog_linked EQUAL -1)
    find_library(_SYS_GLOG NAMES glog QUIET)
    find_library(_SYS_GFLAGS NAMES gflags QUIET)
    if(_SYS_GLOG)
      target_link_libraries(velox_imported INTERFACE ${_SYS_GLOG})
      message(STATUS "glog (system): ${_SYS_GLOG}")
    endif()
    if(_SYS_GFLAGS)
      target_link_libraries(velox_imported INTERFACE ${_SYS_GFLAGS})
      message(STATUS "gflags (system): ${_SYS_GFLAGS}")
    endif()
  endif()
endif()

# 7) AWS SDK (required by Velox S3 connector — force-loaded into both dylib and tests)
# AWS SDK location is determined from Velox's CMakeCache (AWSSDK_DIR) since Velox
# already installed it during its own build. No need to duplicate into PANGROVA_DEPS_PREFIX.
if(VELOX_BUILD_DIR AND EXISTS "${VELOX_BUILD_DIR}/CMakeCache.txt")
  file(STRINGS "${VELOX_BUILD_DIR}/CMakeCache.txt" _AWSSDK_LINES REGEX "^AWSSDK_DIR:PATH=")
  if(_AWSSDK_LINES)
    list(GET _AWSSDK_LINES 0 _AWSSDK_LINE)
    string(REGEX REPLACE "^AWSSDK_DIR:PATH=" "" _AWSSDK_CMAKE_DIR "${_AWSSDK_LINE}")
    # AWSSDK_DIR points to <prefix>/lib/cmake/AWSSDK — lib is at <prefix>/lib/
    get_filename_component(_AWS_PREFIX "${_AWSSDK_CMAKE_DIR}" DIRECTORY)  # lib/cmake
    get_filename_component(_AWS_PREFIX "${_AWS_PREFIX}" DIRECTORY)         # lib
    get_filename_component(_AWS_PREFIX "${_AWS_PREFIX}" DIRECTORY)         # <prefix>
    if(EXISTS "${_AWS_PREFIX}/lib")
      file(GLOB _AWS_LIBS "${_AWS_PREFIX}/lib/libaws-*.a")
      if(_AWS_LIBS)
        target_link_libraries(velox_imported INTERFACE ${_AWS_LIBS})
        if(APPLE)
          target_link_libraries(velox_imported INTERFACE
            "-framework Security"
            "-framework CoreFoundation"
            "-framework Network"
            "-lcurl"
          )
        else()
          target_link_libraries(velox_imported INTERFACE "-lcurl" "-lssl" "-lcrypto")
        endif()
        list(LENGTH _AWS_LIBS _N_AWS)
        message(STATUS "AWS SDK libs: ${_N_AWS} .a files from ${_AWS_PREFIX}/lib/")
      endif()
    endif()
  endif()
endif()
if(NOT _AWS_LIBS)
  # Fallback: check PANGROVA_DEPS_PREFIX
  if(PANGROVA_DEPS_PREFIX)
    file(GLOB _AWS_LIBS "${PANGROVA_DEPS_PREFIX}/lib/libaws-*.a")
    if(_AWS_LIBS)
      target_link_libraries(velox_imported INTERFACE ${_AWS_LIBS})
      if(APPLE)
        target_link_libraries(velox_imported INTERFACE
          "-framework Security" "-framework CoreFoundation" "-lcurl")
      else()
        target_link_libraries(velox_imported INTERFACE "-lcurl" "-lssl" "-lcrypto")
      endif()
      list(LENGTH _AWS_LIBS _N_AWS)
      message(STATUS "AWS SDK libs: ${_N_AWS} .a files from ${PANGROVA_DEPS_PREFIX}/lib/")
    else()
      message(WARNING "AWS SDK NOT found — Velox S3 connector symbols will be UNRESOLVED. "
        "Run: bin/install-aws-sdk.sh")
    endif()
  endif()
endif()

# 8) ABI compatibility: Velox headers use #ifdef NDEBUG to inline TestValue::adjust().
# When Velox is built in Release mode (NDEBUG defined) but we build in Debug mode,
# the compiler generates an external reference to adjust() that doesn't exist in
# libvelox.a. Force NDEBUG for all Velox header consumers to match Velox's ABI.
# This only affects Velox's inline guards — our own code still compiles in Debug mode.
target_compile_definitions(velox_imported INTERFACE NDEBUG)
message(STATUS "Velox ABI compat: NDEBUG forced (Velox built in Release mode)")

# 9) Apache Thrift (required by Velox Hive connector — force-loaded from libvelox.a)
# Thrift is built by Arrow's ExternalProject during Velox build.
if(VELOX_BUILD_DIR AND EXISTS "${VELOX_BUILD_DIR}/CMakeCache.txt")
  # Find thrift_ep-install from Velox's Arrow build tree
  set(_THRIFT_SEARCH_DIRS
    "${VELOX_BUILD_DIR}/CMake/resolve_dependency_modules/arrow/arrow_ep/src/arrow_ep-build/thrift_ep-install"
  )
  foreach(_td ${_THRIFT_SEARCH_DIRS})
    if(EXISTS "${_td}/lib/libthrift.a")
      if(NOT EXISTS "${_td}/include/thrift/TApplicationException.h")
        message(FATAL_ERROR "The selected Velox Thrift archive requires its matching headers: ${_td}/include")
      endif()
      target_include_directories(velox_imported SYSTEM INTERFACE "${_td}/include")
      target_link_libraries(velox_imported INTERFACE "${_td}/lib/libthrift.a")
      message(STATUS "Thrift: ${_td}/lib/libthrift.a")
      break()
    endif()
  endforeach()
endif()

set(VELOX_FOUND TRUE)
