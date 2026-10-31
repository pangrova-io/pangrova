#pragma once

#include <cstdint>

namespace pangrova::memory {

// Memory pressure levels aligned with Java-side definition
// (Spec 05 worker-resource MemoryPressure enum, 06-resource-management.md §3).
enum class MemoryPressure {
  NONE,      // < low%  — normal operation
  LOW,       // low-medium% — reduce prefetch window
  MEDIUM,    // medium-high% — stop prefetch, reduce cache
  HIGH,      // high-critical% — trigger spill on largest consumers
  CRITICAL   // >= critical% — aggressive spill, reject new allocations
};

// Runtime-configurable thresholds. Initialized from Java during nativeInit()
// via NativeConfig proto. Falls back to defaults if not configured.
struct PressureThresholds {
  double low = 50.0;
  double medium = 70.0;
  double high = 85.0;
  double critical = 95.0;
};

// Global thresholds instance — written once during init, read on every pressure check.
inline PressureThresholds& pressureThresholds() {
  static PressureThresholds instance;
  return instance;
}

// Default constants for use in code that needs compile-time values (e.g., tests).
constexpr double kPressureThresholdLow = 50.0;
constexpr double kPressureThresholdMedium = 70.0;
constexpr double kPressureThresholdHigh = 85.0;
constexpr double kPressureThresholdCritical = 95.0;

// Configure thresholds at startup. Must be called before any pressure checks.
inline void configurePressureThresholds(double low, double medium,
                                        double high, double critical) {
  auto& t = pressureThresholds();
  t.low = low;
  t.medium = medium;
  t.high = high;
  t.critical = critical;
}

// Compute pressure level from usage percentage [0, 100].
inline MemoryPressure computePressure(double usagePercent) {
  const auto& t = pressureThresholds();
  if (usagePercent >= t.critical) {
    return MemoryPressure::CRITICAL;
  }
  if (usagePercent >= t.high) {
    return MemoryPressure::HIGH;
  }
  if (usagePercent >= t.medium) {
    return MemoryPressure::MEDIUM;
  }
  if (usagePercent >= t.low) {
    return MemoryPressure::LOW;
  }
  return MemoryPressure::NONE;
}

}  // namespace pangrova::memory
