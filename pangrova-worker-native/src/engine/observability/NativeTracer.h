#pragma once

#include <atomic>
#include <memory>
#include <string>

namespace pangrova::observability {

class SpanScope;

/**
 * OpenTelemetry C++ SDK integration for native tracing.
 * Initialized via JNI_OnLoad with OTLP gRPC exporter.
 *
 * All static methods are safe to call before initialize() — they
 * degrade to no-op when the tracer is not initialized.
 */
class NativeTracer {
public:
    static void initialize(const std::string& otlpEndpoint,
                           const std::string& serviceName);

    static void shutdown();

    // Returns true if initialize() has been called successfully.
    static bool isInitialized();

    static std::unique_ptr<SpanScope> startSpanWithParent(
        const std::string& traceIdHex,
        const std::string& spanIdHex,
        const std::string& spanName);

    static std::unique_ptr<SpanScope> startSpan(const std::string& spanName);

private:
    NativeTracer() = delete;
    static std::atomic<bool> initialized_;
};

} // namespace pangrova::observability
