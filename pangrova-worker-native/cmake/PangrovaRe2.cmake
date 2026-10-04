# The gRPC package installs one complete RE2 package. Velox and Pangrova must
# compile against that package, never its headers plus a different archive.
set(_PANGROVA_RE2_DIR "${PANGROVA_DEPS_PREFIX}/lib/cmake/re2")
if(NOT EXISTS "${_PANGROVA_RE2_DIR}/re2Config.cmake")
  message(FATAL_ERROR "Canonical RE2 package is missing; rebuild the native dependencies")
endif()
file(STRINGS "${VELOX_BUILD_DIR}/CMakeCache.txt" _VELOX_RE2_PACKAGE
  REGEX "^re2_DIR:(PATH|UNINITIALIZED)=")
if(NOT _VELOX_RE2_PACKAGE)
  message(FATAL_ERROR "Velox does not record its selected RE2 package")
endif()
list(GET _VELOX_RE2_PACKAGE 0 _VELOX_RE2_PACKAGE)
string(REGEX REPLACE "^re2_DIR:(PATH|UNINITIALIZED)=" "" _VELOX_RE2_PACKAGE "${_VELOX_RE2_PACKAGE}")
file(REAL_PATH "${_PANGROVA_RE2_DIR}" _PANGROVA_RE2_REAL)
file(REAL_PATH "${_VELOX_RE2_PACKAGE}" _VELOX_RE2_REAL)
if(NOT _PANGROVA_RE2_REAL STREQUAL _VELOX_RE2_REAL)
  message(FATAL_ERROR
    "Velox RE2 package differs from the canonical package; rebuild Velox with re2_DIR=${_PANGROVA_RE2_DIR}")
endif()
find_package(absl CONFIG REQUIRED
  PATHS "${PANGROVA_DEPS_PREFIX}/lib/cmake/absl" NO_DEFAULT_PATH)
find_package(re2 CONFIG REQUIRED PATHS "${_PANGROVA_RE2_DIR}" NO_DEFAULT_PATH)
get_target_property(_PANGROVA_RE2_CONFIGURATIONS re2::re2 IMPORTED_CONFIGURATIONS)
foreach(_PANGROVA_RE2_CONFIGURATION ${_PANGROVA_RE2_CONFIGURATIONS})
  string(TOUPPER "${_PANGROVA_RE2_CONFIGURATION}" _PANGROVA_RE2_CONFIGURATION)
  get_target_property(_PANGROVA_RE2_ARCHIVE re2::re2
    "IMPORTED_LOCATION_${_PANGROVA_RE2_CONFIGURATION}")
  if(_PANGROVA_RE2_ARCHIVE MATCHES "\\.a$")
    execute_process(COMMAND "${CMAKE_AR}" t "${_PANGROVA_RE2_ARCHIVE}"
      OUTPUT_VARIABLE _PANGROVA_RE2_MEMBERS RESULT_VARIABLE _PANGROVA_RE2_AR_RESULT)
    if(NOT _PANGROVA_RE2_AR_RESULT EQUAL 0)
      message(FATAL_ERROR "Cannot inspect canonical RE2 archive: ${_PANGROVA_RE2_ARCHIVE}")
    endif()
    string(REPLACE "\r" "" _PANGROVA_RE2_MEMBERS "${_PANGROVA_RE2_MEMBERS}")
    string(REPLACE "\n" ";" _PANGROVA_RE2_MEMBERS "${_PANGROVA_RE2_MEMBERS}")
    list(FILTER _PANGROVA_RE2_MEMBERS EXCLUDE REGEX "^$")
    list(LENGTH _PANGROVA_RE2_MEMBERS _PANGROVA_RE2_MEMBER_COUNT)
    list(REMOVE_DUPLICATES _PANGROVA_RE2_MEMBERS)
    list(LENGTH _PANGROVA_RE2_MEMBERS _PANGROVA_RE2_UNIQUE_COUNT)
    if(NOT _PANGROVA_RE2_MEMBER_COUNT EQUAL _PANGROVA_RE2_UNIQUE_COUNT)
      message(FATAL_ERROR
        "Canonical RE2 archive contains duplicate object members; rebuild the complete package")
    endif()
  endif()
endforeach()
try_compile(_PANGROVA_RE2_ABI_MATCHES
  "${CMAKE_CURRENT_BINARY_DIR}/re2-contract-check"
  "${CMAKE_CURRENT_LIST_DIR}/dependency-checks/re2"
  PangrovaRe2Contract pangrova_re2_contract
  CMAKE_FLAGS "-DPANGROVA_DEPS_PREFIX=${PANGROVA_DEPS_PREFIX}"
              "-Dabsl_DIR:PATH=${absl_DIR}"
              "-DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}"
              "-DCMAKE_EXE_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS}"
  OUTPUT_VARIABLE _PANGROVA_RE2_CHECK_OUTPUT)
if(NOT _PANGROVA_RE2_ABI_MATCHES)
  message(FATAL_ERROR "Canonical RE2 header/library ABI mismatch:\n${_PANGROVA_RE2_CHECK_OUTPUT}")
endif()
if(CMAKE_CROSSCOMPILING AND NOT CMAKE_CROSSCOMPILING_EMULATOR)
  message(FATAL_ERROR "Canonical RE2 runtime ABI verification requires a target emulator when cross-compiling")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env
          "ASAN_OPTIONS=detect_leaks=1:halt_on_error=1"
          "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_LIST_DIR}/../sanitizer-suppressions/asan.supp"
          ${CMAKE_CROSSCOMPILING_EMULATOR}
          "${CMAKE_CURRENT_BINARY_DIR}/re2-contract-check/pangrova_re2_contract${CMAKE_EXECUTABLE_SUFFIX}"
  RESULT_VARIABLE _PANGROVA_RE2_RUNTIME_RESULT
  OUTPUT_VARIABLE _PANGROVA_RE2_RUNTIME_OUTPUT
  ERROR_VARIABLE _PANGROVA_RE2_RUNTIME_ERROR
  TIMEOUT 30)
if(NOT "${_PANGROVA_RE2_RUNTIME_RESULT}" STREQUAL "0")
  message(FATAL_ERROR
    "Canonical RE2 runtime ABI mismatch (${_PANGROVA_RE2_RUNTIME_RESULT}):\n"
    "${_PANGROVA_RE2_RUNTIME_OUTPUT}${_PANGROVA_RE2_RUNTIME_ERROR}")
endif()
