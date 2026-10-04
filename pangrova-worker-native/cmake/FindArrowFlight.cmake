# ==============================================================================
# FindArrowFlight.cmake — Locate Arrow Flight for native shuffle transport
#
# Input (set via -D, env var, or CMakeUserPresets.json):
#   ARROW_FLIGHT_HOME  — Arrow Flight install prefix (no default — must be explicit)
#
# Output:
#   arrow_flight_imported  — SHARED IMPORTED target (if found)
#   ARROW_FLIGHT_FOUND     — TRUE/FALSE
#   ARROW_FLIGHT_LIB       — Path to libarrow_flight.dylib/so
# ==============================================================================

# Read from environment if not set via -D
if(NOT ARROW_FLIGHT_HOME AND DEFINED ENV{ARROW_FLIGHT_HOME})
  set(ARROW_FLIGHT_HOME "$ENV{ARROW_FLIGHT_HOME}" CACHE PATH "Arrow Flight install prefix")
endif()

# Auto-detect from Arrow's install prefix if ARROW_FLIGHT_HOME not explicitly set.
# Arrow and Arrow Flight are typically co-installed in the same prefix.
if(NOT ARROW_FLIGHT_HOME)
  # Locate the co-installed package; headers alone are not an execution runtime.
  get_target_property(_arrow_loc ${PANGROVA_ARROW_TARGET} IMPORTED_LOCATION)
  if(_arrow_loc)
    get_filename_component(_arrow_lib_dir "${_arrow_loc}" DIRECTORY)
    get_filename_component(_arrow_prefix "${_arrow_lib_dir}" DIRECTORY)
    if(EXISTS "${_arrow_prefix}/include/arrow/flight/types.h")
      set(ARROW_FLIGHT_HOME "${_arrow_prefix}")
      message(STATUS "Arrow Flight: auto-detected ARROW_FLIGHT_HOME=${_arrow_prefix}")
    endif()
  endif()
  # Fallback: check PANGROVA_DEPS_PREFIX directly (Arrow headers co-located there)
  if(NOT ARROW_FLIGHT_HOME AND PANGROVA_DEPS_PREFIX
      AND EXISTS "${PANGROVA_DEPS_PREFIX}/include/arrow/flight/types.h")
    set(ARROW_FLIGHT_HOME "${PANGROVA_DEPS_PREFIX}")
    message(STATUS "Arrow Flight: detected from PANGROVA_DEPS_PREFIX=${PANGROVA_DEPS_PREFIX}")
  endif()
endif()

if(NOT ARROW_FLIGHT_HOME)
  message(FATAL_ERROR "Canonical Native execution requires the Arrow Flight package")
endif()

find_library(ARROW_FLIGHT_LIB
  NAMES arrow_flight
  PATHS "${ARROW_FLIGHT_HOME}/lib"
  NO_DEFAULT_PATH
)

if(ARROW_FLIGHT_LIB)
  # find_library retains an earlier cached library when the package prefix
  # changes. Never admit a Release library under a new sanitizer package.
  if(NOT EXISTS "${ARROW_FLIGHT_LIB}")
    message(FATAL_ERROR "Selected Arrow Flight library does not exist: ${ARROW_FLIGHT_LIB}")
  endif()
  file(REAL_PATH "${ARROW_FLIGHT_HOME}/lib" _FLIGHT_PACKAGE_LIB_DIR)
  file(REAL_PATH "${ARROW_FLIGHT_LIB}" _FLIGHT_LIBRARY_REAL)
  get_filename_component(_FLIGHT_LIBRARY_DIR "${_FLIGHT_LIBRARY_REAL}" DIRECTORY)
  if(NOT _FLIGHT_LIBRARY_DIR STREQUAL _FLIGHT_PACKAGE_LIB_DIR)
    message(FATAL_ERROR
      "Flight library differs from the selected package; clear ARROW_FLIGHT_LIB and reconfigure: ${ARROW_FLIGHT_LIB}")
  endif()
  # Detect static vs shared library
  string(REGEX MATCH "\\.(a|lib)$" _IS_STATIC "${ARROW_FLIGHT_LIB}")
  if(PANGROVA_PRODUCTION_BUILD AND NOT _IS_STATIC)
    message(FATAL_ERROR "Linux production cannot link shared Arrow Flight: ${ARROW_FLIGHT_LIB}")
  endif()
  if(_IS_STATIC)
    add_library(arrow_flight_imported STATIC IMPORTED)
  else()
    add_library(arrow_flight_imported SHARED IMPORTED)
  endif()
  set_target_properties(arrow_flight_imported PROPERTIES
    IMPORTED_LOCATION "${ARROW_FLIGHT_LIB}"
  )
  if(EXISTS "${ARROW_FLIGHT_HOME}/include")
    set_target_properties(arrow_flight_imported PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${ARROW_FLIGHT_HOME}/include"
    )
  endif()
  # The server builder hook calls gRPC directly even with shared Flight.
  # Its link closure must not depend on enabling the telemetry exporter.
  find_package(gRPC REQUIRED CONFIG
    PATHS "${PANGROVA_DEPS_PREFIX}/lib/cmake/grpc" NO_DEFAULT_PATH)
  set_target_properties(arrow_flight_imported PROPERTIES
    INTERFACE_LINK_LIBRARIES "gRPC::grpc++;gRPC::grpc"
  )
  message(STATUS "Arrow Flight: linked canonical gRPC server API dependency")
  set(ARROW_FLIGHT_FOUND TRUE)
  message(STATUS "Arrow Flight found (full): ${ARROW_FLIGHT_LIB}")
else()
  message(FATAL_ERROR "Canonical Native execution requires the Arrow Flight library under ${ARROW_FLIGHT_HOME}")
endif()
