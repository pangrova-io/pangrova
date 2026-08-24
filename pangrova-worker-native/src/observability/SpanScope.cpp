// SpanScope: RAII guard for span lifecycle.
// When HAS_OTEL_CPP is defined, wraps a real OpenTelemetry Span with
// parent context propagation from Java-side traceId/spanId.
// Otherwise, provides lightweight timing-only tracking.
#include "observability/SpanScope.h"

#include <algorithm>
#include <random>

#ifdef HAS_OTEL_CPP
#include "opentelemetry/trace/provider.h"
#include "opentelemetry/trace/span.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/tracer.h"
#include "opentelemetry/context/context.h"
#endif

namespace pangrova::observability {

namespace {

/// Thread-local random engine for span ID generation.
/// P2-OBS-8: Seeded with std::random_device for proper entropy instead of
/// steady_clock which can produce duplicate span IDs across threads that
/// start at similar times.
std::string randomHex(size_t bytes) {
    static thread_local std::mt19937_64 rng(std::random_device{}());
    static constexpr char kHexChars[] = "0123456789abcdef";

    std::string result;
    result.reserve(bytes * 2);
    std::uniform_int_distribution<int> dist(0, 15);
    for (size_t i = 0; i < bytes * 2; ++i) {
        result.push_back(kHexChars[dist(rng)]);
    }
    return result;
}

#ifdef HAS_OTEL_CPP
// Convert hex string to byte array.
std::vector<uint8_t> hexToBytes(const std::string& hex) {
    std::vector<uint8_t> bytes;
    bytes.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2) {
        uint8_t byte = 0;
        for (int j = 0; j < 2; ++j) {
            char c = hex[i + j];
            byte <<= 4;
            if (c >= '0' && c <= '9') byte |= (c - '0');
            else if (c >= 'a' && c <= 'f') byte |= (c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') byte |= (c - 'A' + 10);
        }
        bytes.push_back(byte);
    }
    return bytes;
}
#endif

} // namespace

SpanScope::SpanScope() = default;

SpanScope::SpanScope(const std::string& traceIdHex,
                     const std::string& parentSpanIdHex,
                     const std::string& spanName)
    : trace_id_hex_(traceIdHex),
      span_id_hex_(generateRandomHex(8)),
      parent_span_id_hex_(parentSpanIdHex),
      span_name_(spanName),
      start_time_(std::chrono::steady_clock::now()),
      valid_(true),
      ended_(false) {
#ifdef HAS_OTEL_CPP
    // Create a real OTel child span linked to the Java-side parent.
    auto tracer = opentelemetry::trace::Provider::GetTracerProvider()
                      ->GetTracer("pangrova-native", "0.1.0");
    if (tracer) {
        opentelemetry::trace::StartSpanOptions opts;
        // Reconstruct parent SpanContext from hex IDs.
        if (!parentSpanIdHex.empty()) {
            auto traceBytes = hexToBytes(traceIdHex);
            auto spanBytes = hexToBytes(parentSpanIdHex);
            if (traceBytes.size() == 16 && spanBytes.size() == 8) {
                opentelemetry::trace::TraceId traceId(
                    opentelemetry::nostd::span<const uint8_t, 16>(traceBytes.data(), 16));
                opentelemetry::trace::SpanId spanId(
                    opentelemetry::nostd::span<const uint8_t, 8>(spanBytes.data(), 8));
                opentelemetry::trace::SpanContext parentCtx(
                    traceId, spanId,
                    opentelemetry::trace::TraceFlags(
                        opentelemetry::trace::TraceFlags::kIsSampled),
                    true);
                opts.parent = parentCtx;
            }
        }
        otel_span_ = tracer->StartSpan(spanName, opts);
        // Update span_id_hex_ with the real OTel span ID.
        if (otel_span_) {
            auto ctx = otel_span_->GetContext();
            char buf[17] = {};
            ctx.span_id().ToLowerBase16(
                opentelemetry::nostd::span<char, 16>(buf, 16));
            span_id_hex_ = std::string(buf, 16);
        }
    }
#endif
}

SpanScope::SpanScope(const std::string& spanName)
    : trace_id_hex_(generateRandomHex(16)),
      span_id_hex_(generateRandomHex(8)),
      span_name_(spanName),
      start_time_(std::chrono::steady_clock::now()),
      valid_(true),
      ended_(false) {
#ifdef HAS_OTEL_CPP
    auto tracer = opentelemetry::trace::Provider::GetTracerProvider()
                      ->GetTracer("pangrova-native", "0.1.0");
    if (tracer) {
        otel_span_ = tracer->StartSpan(spanName);
        if (otel_span_) {
            auto ctx = otel_span_->GetContext();
            // Update hex IDs with real OTel values.
            char traceBuf[33] = {};
            ctx.trace_id().ToLowerBase16(
                opentelemetry::nostd::span<char, 32>(traceBuf, 32));
            trace_id_hex_ = std::string(traceBuf, 32);
            char spanBuf[17] = {};
            ctx.span_id().ToLowerBase16(
                opentelemetry::nostd::span<char, 16>(spanBuf, 16));
            span_id_hex_ = std::string(spanBuf, 16);
        }
    }
#endif
}

SpanScope::~SpanScope() {
    if (valid_ && !ended_) {
        ended_ = true;
#ifdef HAS_OTEL_CPP
        if (otel_span_) {
            otel_span_->End();
        }
#endif
    }
}

SpanScope::SpanScope(SpanScope&& other) noexcept
    : trace_id_hex_(std::move(other.trace_id_hex_)),
      span_id_hex_(std::move(other.span_id_hex_)),
      parent_span_id_hex_(std::move(other.parent_span_id_hex_)),
      span_name_(std::move(other.span_name_)),
      start_time_(other.start_time_),
      valid_(other.valid_),
      ended_(other.ended_)
#ifdef HAS_OTEL_CPP
      , otel_span_(std::move(other.otel_span_))
#endif
{
    other.valid_ = false;
}

SpanScope& SpanScope::operator=(SpanScope&& other) noexcept {
    if (this != &other) {
        if (valid_ && !ended_) {
            ended_ = true;
#ifdef HAS_OTEL_CPP
            if (otel_span_) {
                otel_span_->End();
            }
#endif
        }
        trace_id_hex_ = std::move(other.trace_id_hex_);
        span_id_hex_ = std::move(other.span_id_hex_);
        parent_span_id_hex_ = std::move(other.parent_span_id_hex_);
        span_name_ = std::move(other.span_name_);
        start_time_ = other.start_time_;
        valid_ = other.valid_;
        ended_ = other.ended_;
#ifdef HAS_OTEL_CPP
        otel_span_ = std::move(other.otel_span_);
#endif
        other.valid_ = false;
    }
    return *this;
}

bool SpanScope::isValid() const { return valid_; }

const std::string& SpanScope::traceIdHex() const { return trace_id_hex_; }

const std::string& SpanScope::spanIdHex() const { return span_id_hex_; }

const std::string& SpanScope::spanName() const { return span_name_; }

bool SpanScope::hasEnded() const { return ended_; }

std::string SpanScope::generateRandomHex(size_t bytes) {
    return randomHex(bytes);
}

} // namespace pangrova::observability
