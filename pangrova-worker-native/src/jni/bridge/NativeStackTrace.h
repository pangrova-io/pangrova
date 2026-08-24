// Utility for capturing C++ stack traces at the point of failure.
// Used by the error propagation path to provide native debugging context
// to Java without requiring native core dumps.
#pragma once

#include <string>

namespace pangrova::jni {

// Captures the current C++ call stack as a human-readable string.
// Uses backtrace()/backtrace_symbols() on platforms that support it.
// Returns an empty string on unsupported platforms or capture failure.
// Thread-safe: no shared mutable state.
std::string captureNativeStackTrace();

}  // namespace pangrova::jni
