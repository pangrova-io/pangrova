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
  # Auto-detect from Arrow's install prefix — check for headers OR library.
  # Headers are sufficient for compilation; the strip script removes Flight .o
  # files so the library is not needed at link time.
  get_target_property(_arrow_loc Arrow::arrow_shared IMPORTED_LOCATION)
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
  set(ARROW_FLIGHT_FOUND FALSE)
  message(STATUS "Arrow Flight: ARROW_FLIGHT_HOME not set — FlightShuffleServer disabled")
  return()
endif()

find_library(ARROW_FLIGHT_LIB
  NAMES arrow_flight
  PATHS "${ARROW_FLIGHT_HOME}/lib"
  NO_DEFAULT_PATH
)

if(ARROW_FLIGHT_LIB)
  # Detect static vs shared library
  string(REGEX MATCH "\\.(a|lib)$" _IS_STATIC "${ARROW_FLIGHT_LIB}")
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
  # Static Arrow Flight requires its gRPC dependency to be linked explicitly.
  # When OTEL is ON, gRPC comes via OpenTelemetry's gRPC::grpc++ target.
  # When OTEL is OFF (sanitizer builds), we must find gRPC ourselves.
  if(_IS_STATIC)
    find_package(gRPC QUIET CONFIG)
    if(gRPC_FOUND)
      set_target_properties(arrow_flight_imported PROPERTIES
        INTERFACE_LINK_LIBRARIES "gRPC::grpc++;gRPC::grpc"
      )
      message(STATUS "Arrow Flight: linked gRPC via find_package (static dep)")
    else()
      # Fallback: link gRPC static libs directly from hermetic deps
      get_filename_component(_flight_lib_dir "${ARROW_FLIGHT_LIB}" DIRECTORY)
      foreach(_grpc_lib libgrpc++.a libgrpc.a)
        if(EXISTS "${_flight_lib_dir}/${_grpc_lib}")
          set_property(TARGET arrow_flight_imported APPEND PROPERTY
            INTERFACE_LINK_LIBRARIES "${_flight_lib_dir}/${_grpc_lib}")
        endif()
      endforeach()
      message(STATUS "Arrow Flight: linked gRPC static libs directly (static dep)")
    endif()
  endif()
  set(ARROW_FLIGHT_FOUND TRUE)
  message(STATUS "Arrow Flight found (full): ${ARROW_FLIGHT_LIB}")
elseif(EXISTS "${ARROW_FLIGHT_HOME}/include/arrow/flight/types.h")
  # Headers-only mode: Flight headers available for compilation but no library
  # to link. The strip script (bin/strip-velox-optional-deps.sh) removes .o
  # files that reference Flight symbols, so no link-time dependency is needed.
  # This allows the codebase to compile cleanly without #ifdef guards everywhere.
  add_library(arrow_flight_imported INTERFACE)
  target_include_directories(arrow_flight_imported INTERFACE "${ARROW_FLIGHT_HOME}/include")
  set(ARROW_FLIGHT_FOUND TRUE)
  set(ARROW_FLIGHT_HEADERS_ONLY TRUE)
  message(STATUS "Arrow Flight found (headers-only): ${ARROW_FLIGHT_HOME}/include")
else()
  set(ARROW_FLIGHT_FOUND FALSE)
  message(WARNING "Arrow Flight NOT found at ${ARROW_FLIGHT_HOME} — FlightShuffleServer disabled")
endif()
