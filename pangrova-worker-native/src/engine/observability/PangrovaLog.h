// PangrovaLog.h: Unified structured logging macro for C++ native module.
// Standardizes on glog with key=value format. Falls back to std::cerr
// when glog is unavailable (e.g., unit test builds without glog).
//
// Usage:
//   PANGROVA_LOG(INFO) << "event=task_started task_id=" << task_id;
//   PANGROVA_LOG(WARNING) << "event=memory_pressure current=" << bytes;
//   PANGROVA_LOG(ERROR) << "event=task_failed error=" << msg;
//   PANGROVA_LOG_DEBUG << "event=batch_detail ...";  // only with --v=1+
//
// Log levels (per pangrova-rules §7):
//   ERROR = human intervention required
//   WARN  = self-healing situation
//   INFO  = state change (start/finish/commit)
//   DEBUG = detail (per-batch, per-row diagnostics)
#pragma once

#if __has_include(<glog/logging.h>)
#include <glog/logging.h>
#define PANGROVA_LOG(level) LOG(level)
#define PANGROVA_VLOG(verbosity) VLOG(verbosity)
#else
#include <iostream>
#define PANGROVA_LOG(level) std::cerr << "[" #level "] "
#define PANGROVA_VLOG(verbosity) \
  if (pangrova::log::debugEnabled()) std::cerr << "[DEBUG] "
namespace pangrova::log {
inline bool debugEnabled() { return false; }
}  // namespace pangrova::log
#endif

// Convenience macros: stream-style wrappers for common log levels.
// Usage: PANGROVA_LOG_INFO << "event=task_started task_id=" << task_id;
#define PANGROVA_LOG_INFO PANGROVA_LOG(INFO)
#define PANGROVA_LOG_WARN PANGROVA_LOG(WARNING)
#define PANGROVA_LOG_ERROR PANGROVA_LOG(ERROR)
#define PANGROVA_LOG_DEBUG PANGROVA_VLOG(1)
