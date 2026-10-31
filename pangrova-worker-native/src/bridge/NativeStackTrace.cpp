// Captures C++ stack traces for error propagation to Java.
// Uses execinfo.h (backtrace/backtrace_symbols) on macOS and glibc Linux.

#include "bridge/NativeStackTrace.h"

#include <cstdlib>
#include <sstream>
#include <string>

#if defined(__APPLE__) || defined(__linux__)
#include <execinfo.h>
#define PANGROVA_HAS_BACKTRACE 1
#else
#define PANGROVA_HAS_BACKTRACE 0
#endif

namespace pangrova::jni {

namespace {
// Maximum stack frames to capture. Deep enough for meaningful context,
// shallow enough to avoid excessive output in error reports.
constexpr int kMaxFrames = 64;

// Number of top frames to skip (captureNativeStackTrace itself + caller).
constexpr int kSkipFrames = 2;
}  // namespace

std::string captureNativeStackTrace() {
#if PANGROVA_HAS_BACKTRACE
  void* frames[kMaxFrames];
  int frame_count = backtrace(frames, kMaxFrames);
  if (frame_count <= kSkipFrames) {
    return {};
  }

  char** symbols = backtrace_symbols(frames, frame_count);
  if (!symbols) {
    return {};
  }

  std::ostringstream oss;
  for (int i = kSkipFrames; i < frame_count; ++i) {
    oss << symbols[i];
    if (i < frame_count - 1) {
      oss << '\n';
    }
  }

  free(symbols);
  return oss.str();
#else
  return {};
#endif
}

}  // namespace pangrova::jni
