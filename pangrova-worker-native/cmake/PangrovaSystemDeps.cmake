# ==============================================================================
# PangrovaSystemDeps.cmake — System libraries required by Velox/folly/glog
#
# Adds system dependencies to the velox_imported INTERFACE target.
# Handles both macOS (Homebrew) and Linux (system packages) transparently.
# ==============================================================================

# --- Helper: find Homebrew package (macOS only, no-op on Linux) ---
function(pangrova_find_brew_package PKG_NAME)
  if(NOT APPLE)
    return()
  endif()
  execute_process(
    COMMAND brew --prefix ${PKG_NAME}
    OUTPUT_VARIABLE _prefix
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
    RESULT_VARIABLE _rc
  )
  if(_rc EQUAL 0 AND EXISTS "${_prefix}")
    target_include_directories(velox_imported INTERFACE "${_prefix}/include")
    file(GLOB _static_libs "${_prefix}/lib/*.a")
    if(_static_libs)
      target_link_libraries(velox_imported INTERFACE ${_static_libs})
    else()
      file(GLOB _dylibs "${_prefix}/lib/*.dylib")
      if(_dylibs)
        target_link_libraries(velox_imported INTERFACE ${_dylibs})
      endif()
    endif()
  endif()
endfunction()

# --- OpenSSL ---
find_package(OpenSSL QUIET)
if(OpenSSL_FOUND)
  target_link_libraries(velox_imported INTERFACE OpenSSL::SSL OpenSSL::Crypto)
endif()

# --- bzip2 (required by folly Compression.cpp) ---
find_package(BZip2 QUIET)
if(BZIP2_FOUND)
  target_link_libraries(velox_imported INTERFACE BZip2::BZip2)
else()
  # Fallback: link directly (available in macOS SDK and most Linux systems)
  target_link_libraries(velox_imported INTERFACE "-lbz2")
endif()

# --- zlib (required by Velox Parquet compression, folly, AWS SDK) ---
find_package(ZLIB QUIET)
if(ZLIB_FOUND)
  target_link_libraries(velox_imported INTERFACE ZLIB::ZLIB)
else()
  target_link_libraries(velox_imported INTERFACE "-lz")
endif()

# --- Homebrew packages (macOS) / system packages (Linux) ---
foreach(_pkg double-conversion libsodium lz4 zstd snappy xz xxhash)
  pangrova_find_brew_package(${_pkg})
endforeach()

# Linux: find system-installed libraries via pkg-config or find_library
if(NOT APPLE)
  include(FindPkgConfig)
  # These packages are typically installed via apt/yum on Linux CI
  foreach(_pkg double-conversion libevent libsodium)
    pkg_check_modules(_PC_${_pkg} QUIET ${_pkg})
    if(_PC_${_pkg}_FOUND)
      target_include_directories(velox_imported INTERFACE ${_PC_${_pkg}_INCLUDE_DIRS})
      if(PANGROVA_PRODUCTION_BUILD)
        target_link_directories(velox_imported INTERFACE ${_PC_${_pkg}_STATIC_LIBRARY_DIRS})
        foreach(_static_name ${_PC_${_pkg}_STATIC_LIBRARIES})
          string(MAKE_C_IDENTIFIER "${_pkg}_${_static_name}" _static_key)
          find_library(_STATIC_${_static_key} NAMES ${_static_name}
            HINTS ${_PC_${_pkg}_STATIC_LIBRARY_DIRS} REQUIRED)
          target_link_libraries(velox_imported INTERFACE ${_STATIC_${_static_key}})
        endforeach()
      else()
        target_link_libraries(velox_imported INTERFACE ${_PC_${_pkg}_LINK_LIBRARIES})
      endif()
    endif()
  endforeach()
  # Libraries that pkg-config may not cover — use find_library
  foreach(_lib lz4 zstd snappy lzma xxhash)
    find_library(_FOUND_${_lib} NAMES ${_lib} QUIET)
    if(_FOUND_${_lib})
      target_link_libraries(velox_imported INTERFACE ${_FOUND_${_lib}})
    endif()
  endforeach()
  # libevent components on Linux
  foreach(_comp event_core event_extra event_pthreads event_openssl)
    find_library(_FOUND_${_comp} NAMES ${_comp} QUIET)
    if(_FOUND_${_comp})
      target_link_libraries(velox_imported INTERFACE ${_FOUND_${_comp}})
    endif()
  endforeach()
endif()

# --- libevent: link individual components only (avoid duplicate symbols) ---
if(APPLE)
  execute_process(
    COMMAND brew --prefix libevent
    OUTPUT_VARIABLE _libevent_prefix
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
    RESULT_VARIABLE _libevent_rc
  )
  if(_libevent_rc EQUAL 0 AND EXISTS "${_libevent_prefix}")
    target_include_directories(velox_imported INTERFACE "${_libevent_prefix}/include")
    foreach(_comp core extra pthreads openssl)
      if(EXISTS "${_libevent_prefix}/lib/libevent_${_comp}.a")
        target_link_libraries(velox_imported INTERFACE "${_libevent_prefix}/lib/libevent_${_comp}.a")
      endif()
    endforeach()
  endif()
endif()
# Linux libevent is handled in the pkg-config / find_library block above.

# --- ICU (keg-only on macOS) ---
if(APPLE)
  execute_process(
    COMMAND brew --prefix icu4c
    OUTPUT_VARIABLE _icu_prefix
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
    RESULT_VARIABLE _icu_rc
  )
  if(_icu_rc EQUAL 0 AND EXISTS "${_icu_prefix}")
    target_include_directories(velox_imported INTERFACE "${_icu_prefix}/include")
    file(GLOB _icu_libs "${_icu_prefix}/lib/libicu*.a")
    if(_icu_libs)
      target_link_libraries(velox_imported INTERFACE ${_icu_libs})
    else()
      file(GLOB _icu_dylibs "${_icu_prefix}/lib/libicu*.dylib")
      target_link_libraries(velox_imported INTERFACE ${_icu_dylibs})
    endif()
  endif()
else()
  find_package(ICU COMPONENTS i18n uc data QUIET)
  if(ICU_FOUND)
    target_link_libraries(velox_imported INTERFACE ICU::i18n ICU::uc ICU::data)
  endif()
endif()

# --- curl (required by Velox S3 filesystem adapter) ---
if(APPLE)
  execute_process(
    COMMAND brew --prefix curl
    OUTPUT_VARIABLE _curl_prefix
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
    RESULT_VARIABLE _curl_rc
  )
  if(_curl_rc EQUAL 0 AND EXISTS "${_curl_prefix}")
    target_include_directories(velox_imported INTERFACE "${_curl_prefix}/include")
    if(EXISTS "${_curl_prefix}/lib/libcurl.a")
      target_link_libraries(velox_imported INTERFACE "${_curl_prefix}/lib/libcurl.a")
    else()
      find_package(CURL QUIET)
      if(CURL_FOUND)
        target_link_libraries(velox_imported INTERFACE CURL::libcurl)
      endif()
    endif()
  endif()
else()
  if(PANGROVA_PRODUCTION_BUILD)
    pkg_check_modules(_PRODUCTION_CURL REQUIRED libcurl)
    target_include_directories(velox_imported INTERFACE ${_PRODUCTION_CURL_INCLUDE_DIRS})
    foreach(_curl_library ${_PRODUCTION_CURL_STATIC_LIBRARIES})
      string(MAKE_C_IDENTIFIER "${_curl_library}" _curl_key)
      find_library(_CURL_STATIC_${_curl_key} NAMES ${_curl_library}
        HINTS ${_PRODUCTION_CURL_STATIC_LIBRARY_DIRS} REQUIRED)
      target_link_libraries(velox_imported INTERFACE ${_CURL_STATIC_${_curl_key}})
    endforeach()
  else()
    find_package(CURL QUIET)
    if(CURL_FOUND)
      target_link_libraries(velox_imported INTERFACE CURL::libcurl)
    endif()
  endif()
endif()

# --- libxml2 (may be required by AWS SDK) ---
if(APPLE)
  execute_process(
    COMMAND brew --prefix libxml2
    OUTPUT_VARIABLE _libxml2_prefix
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
    RESULT_VARIABLE _libxml2_rc
  )
  if(_libxml2_rc EQUAL 0 AND EXISTS "${_libxml2_prefix}")
    target_include_directories(velox_imported INTERFACE "${_libxml2_prefix}/include/libxml2")
    if(EXISTS "${_libxml2_prefix}/lib/libxml2.a")
      target_link_libraries(velox_imported INTERFACE "${_libxml2_prefix}/lib/libxml2.a")
    else()
      file(GLOB _libxml2_dylibs "${_libxml2_prefix}/lib/libxml2*.dylib")
      if(_libxml2_dylibs)
        target_link_libraries(velox_imported INTERFACE ${_libxml2_dylibs})
      endif()
    endif()
  endif()
else()
  find_package(LibXml2 QUIET)
  if(LibXml2_FOUND)
    target_link_libraries(velox_imported INTERFACE LibXml2::LibXml2)
  endif()
endif()

# --- jemalloc: INTENTIONALLY NOT LINKED ---
# folly was built WITHOUT jemalloc (FOLLY_USE_JEMALLOC undefined).
# Weak references resolve at runtime via dlsym; usingJEMalloc() returns false.
# Linking Homebrew jemalloc replaces system allocator → SIGSEGV during dlopen.
message(STATUS "jemalloc: SKIPPED (folly uses system malloc via runtime detection)")

# --- macOS frameworks ---
if(APPLE)
  find_library(COREFOUNDATION_LIB CoreFoundation)
  find_library(SECURITY_LIB Security)
  find_library(IOKIT_LIB IOKit)
  target_link_libraries(velox_imported INTERFACE
    ${COREFOUNDATION_LIB} ${SECURITY_LIB} ${IOKIT_LIB}
    "-framework SystemConfiguration"
  )
else()
  # Linux: threading and dynamic loading libraries
  find_package(Threads REQUIRED)
  target_link_libraries(velox_imported INTERFACE Threads::Threads ${CMAKE_DL_LIBS})
endif()
