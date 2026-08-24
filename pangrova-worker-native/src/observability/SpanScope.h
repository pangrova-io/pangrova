#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <string>

#ifdef HAS_OTEL_CPP
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/trace/span.h"
#endif

namespace pangrova::observability {

/**
 * RAII guard for span lifecycle. Calls End() on destruction.
 * Non-copyable, move-only.
 *
 * Holds lightweight span context (traceId, spanId, name, timestamps)
 * for native-side tracing. When OTel C++ SDK is available, wraps a
 * real OTel Span; otherwise provides a minimal tracking implementation
 * that can be queried from Java side via JNI.
 *
 * @ThreadSafe: instances are not shared across threads.
 */
class SpanScope {
public:
    SpanScope();

    /// Construct with parent context propagation.
    SpanScope(const std::string& traceIdHex,
              const std::string& parentSpanIdHex,
              const std::string& spanName);

    /// Construct a root or child span (no explicit parent).
    explicit SpanScope(const std::string& spanName);

    ~SpanScope();

    SpanScope(const SpanScope&) = delete;
    SpanScope& operator=(const SpanScope&) = delete;
    SpanScope(SpanScope&& other) noexcept;
    SpanScope& operator=(SpanScope&& other) noexcept;

    /// Whether this scope holds a valid span.
    bool isValid() const;

    /// Hex-encoded trace ID (32 chars).
    const std::string& traceIdHex() const;

    /// Hex-encoded span ID (16 chars).
    const std::string& spanIdHex() const;

    /// Span name.
    const std::string& spanName() const;

    /// Whether this span has ended.
    bool hasEnded() const;

private:
    /// Generate a random hex string of given byte length (output is 2*bytes chars).
    static std::string generateRandomHex(size_t bytes);

    std::string trace_id_hex_;
    std::string span_id_hex_;
    std::string parent_span_id_hex_;
    std::string span_name_;
    std::chrono::steady_clock::time_point start_time_;
    bool valid_ = false;
    bool ended_ = false;
#ifdef HAS_OTEL_CPP
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> otel_span_;
#endif
};

} // namespace pangrova::observability
