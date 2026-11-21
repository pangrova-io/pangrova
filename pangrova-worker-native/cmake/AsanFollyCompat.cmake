# Pass this file as CMAKE_PROJECT_folly_INCLUDE when building an isolated
# AddressSanitizer Folly archive on Apple platforms. Folly's ASan option also
# enables UBSan vptr, but the macOS libc++ memory_resource RTTI is not exported.
# Disable only that UBSan subcheck; ASan and container annotations stay active.
if(APPLE AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  cmake_language(DEFER CALL target_compile_options folly_base PRIVATE
    -fno-sanitize=vptr)
endif()
