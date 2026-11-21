# ==============================================================================
# PangrovaProtobuf.cmake — Protobuf compiler and library setup
#
# Resolution strategy (strict, no system fallback):
#   1. PANGROVA_DEPS_PREFIX (hermetic deps built by setup-native-build-env.sh)
#      - hermetic protobuf v4.25.3 / protoc v25.3
#      - ABI-compatible with Velox (rebuilt with same protobuf version)
#   2. Velox-bundled protoc/libprotobuf (fallback)
#   3. FATAL_ERROR (no system protobuf allowed)
#
# All components (Velox, OTel, gRPC, pangrova_proto) use protobuf v4.25.3
# with absl::once_flag ABI. Unified — no version conflicts.
#
# Provides: bundled_protobuf target, PROTOC_EXECUTABLE, pangrova_proto target.
# ==============================================================================

# --- Resolve PANGROVA_DEPS_PREFIX from env if not set via -D ---
if(NOT PANGROVA_DEPS_PREFIX AND DEFINED ENV{PANGROVA_DEPS_PREFIX})
  set(PANGROVA_DEPS_PREFIX "$ENV{PANGROVA_DEPS_PREFIX}" CACHE PATH "Hermetic deps prefix")
endif()

# --- Find protoc ---
set(_PROTOC "")
set(_PB_SOURCE "")

# Priority 1: hermetic deps prefix (v25.3 — unified with Velox rebuild)
if(PANGROVA_DEPS_PREFIX AND EXISTS "${PANGROVA_DEPS_PREFIX}/bin/protoc")
  set(_PROTOC "${PANGROVA_DEPS_PREFIX}/bin/protoc")
  set(_PB_SOURCE "hermetic")
  message(STATUS "protoc (hermetic): ${_PROTOC}")
endif()

# Priority 2: Velox-bundled (fallback)
if(NOT _PROTOC AND VELOX_BUILD_DIR)
  foreach(_pc
    "${VELOX_BUILD_DIR}/_deps/protobuf-build/protoc"
    "${VELOX_BUILD_DIR}/_deps/protobuf-build/protoc-3.21.8.0"
  )
    if(EXISTS "${_pc}")
      set(_PROTOC "${_pc}")
      set(_PB_SOURCE "velox-bundled")
      message(STATUS "protoc (Velox-bundled): ${_PROTOC}")
      break()
    endif()
  endforeach()
endif()

# No system fallback — FATAL_ERROR if not found
if(NOT _PROTOC)
  message(FATAL_ERROR
    "protoc not found in hermetic prefix or Velox build tree.\n"
    "Run: ./pangrova-worker-native/bin/setup-native-build-env.sh\n"
    "Then: source ~/.pangrova/env.sh\n"
    "System protobuf is intentionally blocked to prevent version conflicts.")
endif()
set(PROTOC_EXECUTABLE "${_PROTOC}")

# --- Verify protoc version ---
execute_process(
  COMMAND "${_PROTOC}" --version
  OUTPUT_VARIABLE _PROTOC_VERSION_OUTPUT
  OUTPUT_STRIP_TRAILING_WHITESPACE
  RESULT_VARIABLE _PROTOC_VERSION_RC)
if(_PROTOC_VERSION_RC EQUAL 0)
  message(STATUS "protoc version: ${_PROTOC_VERSION_OUTPUT}")
else()
  message(WARNING "Failed to determine protoc version")
endif()

# --- Find libprotobuf ---
set(_PB_RESOLVED FALSE)

# Priority 1: hermetic deps prefix (has cmake config files, ABI-unified with Velox)
if(PANGROVA_DEPS_PREFIX AND EXISTS "${PANGROVA_DEPS_PREFIX}/lib/cmake/protobuf")
  find_package(Protobuf REQUIRED CONFIG
    PATHS "${PANGROVA_DEPS_PREFIX}" NO_DEFAULT_PATH)
  if(CMAKE_CXX_FLAGS MATCHES "-fsanitize=address")
    if(NOT PANGROVA_ASAN_PROTOBUF_LIB
        OR NOT EXISTS "${PANGROVA_ASAN_PROTOBUF_LIB}")
      message(FATAL_ERROR
        "ASan requires an instrumented v25.3 libprotobuf.a in "
        "PANGROVA_ASAN_PROTOBUF_LIB")
    endif()
    set_target_properties(protobuf::libprotobuf PROPERTIES
      IMPORTED_LOCATION_RELEASE "${PANGROVA_ASAN_PROTOBUF_LIB}")
  endif()
  add_library(bundled_protobuf ALIAS protobuf::libprotobuf)
  message(STATUS "Using hermetic protobuf: ${PANGROVA_DEPS_PREFIX}")
  set(_PB_SOURCE "hermetic")
  set(_PB_RESOLVED TRUE)
endif()

# Priority 2: Velox-bundled static lib (fallback)
if(NOT _PB_RESOLVED AND VELOX_BUILD_DIR
    AND EXISTS "${VELOX_BUILD_DIR}/_deps/protobuf-build/libprotobuf.a")
  set(_PB_LIB "${VELOX_BUILD_DIR}/_deps/protobuf-build/libprotobuf.a")
  set(_PB_INC "")
  foreach(_inc
    "${VELOX_BUILD_DIR}/_deps/protobuf-src/src"
    "$ENV{FETCHCONTENT_SOURCE_DIR_PROTOBUF}/src"
  )
    if(_inc AND EXISTS "${_inc}/google/protobuf/message.h")
      set(_PB_INC "${_inc}")
      break()
    endif()
  endforeach()
  add_library(bundled_protobuf STATIC IMPORTED)
  set_target_properties(bundled_protobuf PROPERTIES IMPORTED_LOCATION "${_PB_LIB}")
  if(_PB_INC)
    set_target_properties(bundled_protobuf PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${_PB_INC}")
  endif()
  message(STATUS "Using Velox-bundled protobuf: ${_PB_LIB}")
  set(_PB_SOURCE "velox-bundled")
  set(_PB_RESOLVED TRUE)
endif()

# No system fallback — FATAL_ERROR if not resolved
if(NOT _PB_RESOLVED)
  message(FATAL_ERROR
    "libprotobuf not found in hermetic prefix or Velox build tree.\n"
    "Run: ./pangrova-worker-native/bin/setup-native-build-env.sh\n"
    "Then: source ~/.pangrova/env.sh\n"
    "System protobuf is intentionally blocked to prevent version conflicts.")
endif()

# --- Resolve well-known types proto path (for protoc --proto_path) ---
set(_WKT_PATH "")
if(PANGROVA_DEPS_PREFIX AND EXISTS "${PANGROVA_DEPS_PREFIX}/include/google/protobuf/any.proto")
  set(_WKT_PATH "${PANGROVA_DEPS_PREFIX}/include")
elseif(_PB_INC AND EXISTS "${_PB_INC}/google/protobuf/any.proto")
  set(_WKT_PATH "${_PB_INC}")
endif()
if(_WKT_PATH)
  message(STATUS "Protobuf well-known types: ${_WKT_PATH}")
else()
  message(WARNING "Protobuf well-known types not found — proto compilation may fail")
endif()

# --- Generate C++ from .proto files ---
# Cross-runtime messages and their pinned third-party IDL imports are owned by
# pangrova-protocol. Native must never keep a mirrored boundary schema.
set(CANONICAL_PROTO_ROOT
    "${CMAKE_CURRENT_SOURCE_DIR}/../pangrova-protocol/src/main/proto")
set(CANONICAL_PROTO_IMPORT_ROOT
    "${CMAKE_CURRENT_SOURCE_DIR}/../pangrova-protocol/src/main/proto-imports")
file(GLOB_RECURSE CANONICAL_BOUNDARY_PROTO_FILES CONFIGURE_DEPENDS
     "${CANONICAL_PROTO_ROOT}/pangrova/protocol/v2/*.proto")
file(GLOB_RECURSE CANONICAL_IMPORT_PROTO_FILES CONFIGURE_DEPENDS
     "${CANONICAL_PROTO_IMPORT_ROOT}/substrait/*.proto")

if(NOT CANONICAL_BOUNDARY_PROTO_FILES)
  message(FATAL_ERROR
    "Canonical Pangrova v2 protocol sources are missing under "
    "${CANONICAL_PROTO_ROOT}/pangrova/protocol/v2. Native builds must be "
    "configured from a checkout or release context containing pangrova-protocol.")
endif()
if(NOT CANONICAL_IMPORT_PROTO_FILES)
  message(FATAL_ERROR
    "Canonical Substrait import IDL is missing under "
    "${CANONICAL_PROTO_IMPORT_ROOT}/substrait.")
endif()
set(PROTO_FILES ${CANONICAL_IMPORT_PROTO_FILES} ${CANONICAL_BOUNDARY_PROTO_FILES})

if(PROTO_FILES)
  set(PROTO_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
  file(MAKE_DIRECTORY ${PROTO_GENERATED_DIR})

  set(PROTO_SRCS)
  set(PROTO_HDRS)

  foreach(PROTO_FILE ${PROTO_FILES})
    string(FIND "${PROTO_FILE}" "${CANONICAL_PROTO_ROOT}/" _CANONICAL_PREFIX)
    string(FIND "${PROTO_FILE}" "${CANONICAL_PROTO_IMPORT_ROOT}/" _IMPORT_PREFIX)
    if(_CANONICAL_PREFIX EQUAL 0)
      file(RELATIVE_PATH PROTO_REL ${CANONICAL_PROTO_ROOT} ${PROTO_FILE})
    elseif(_IMPORT_PREFIX EQUAL 0)
      file(RELATIVE_PATH PROTO_REL ${CANONICAL_PROTO_IMPORT_ROOT} ${PROTO_FILE})
    else()
      message(FATAL_ERROR "IDL source is outside the canonical protocol roots: ${PROTO_FILE}")
    endif()
    get_filename_component(PROTO_REL_DIR ${PROTO_REL} DIRECTORY)
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    if(PROTO_REL_DIR)
      list(APPEND PROTO_SRCS "${PROTO_GENERATED_DIR}/${PROTO_REL_DIR}/${PROTO_NAME}.pb.cc")
      list(APPEND PROTO_HDRS "${PROTO_GENERATED_DIR}/${PROTO_REL_DIR}/${PROTO_NAME}.pb.h")
    else()
      list(APPEND PROTO_SRCS "${PROTO_GENERATED_DIR}/${PROTO_NAME}.pb.cc")
      list(APPEND PROTO_HDRS "${PROTO_GENERATED_DIR}/${PROTO_NAME}.pb.h")
    endif()
  endforeach()

  set(_PROTOC_ARGS
    --proto_path=${CANONICAL_PROTO_ROOT}
    --proto_path=${CANONICAL_PROTO_IMPORT_ROOT}
    --cpp_out=${PROTO_GENERATED_DIR}
  )
  if(_WKT_PATH)
    list(APPEND _PROTOC_ARGS --proto_path=${_WKT_PATH})
  endif()

  add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
    COMMAND ${PROTOC_EXECUTABLE} ${_PROTOC_ARGS} ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Generating Protobuf C++ sources"
  )

  add_library(pangrova_proto ${PROTO_SRCS})
  target_include_directories(pangrova_proto PUBLIC ${PROTO_GENERATED_DIR})
  target_link_libraries(pangrova_proto PUBLIC bundled_protobuf)
endif()
