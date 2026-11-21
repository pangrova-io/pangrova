// NativeTracer: OpenTelemetry C++ SDK integration for native tracing.
// When HAS_OTEL_CPP is defined, creates real OTLP gRPC-exported spans.
// Otherwise, falls back to lightweight SpanScope stubs that record
// timing only (no export).
#include "engine/observability/NativeTracer.h"
#include "engine/observability/SpanScope.h"

#include <stdexcept>

#ifdef HAS_OTEL_CPP
#include "opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_exporter_options.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/sdk/trace/simple_processor_factory.h"
#include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/trace/provider.h"
#include "opentelemetry/trace/tracer.h"
#include "opentelemetry/context/propagation/global_propagator.h"
#endif

#include "engine/observability/PangrovaLog.h"

namespace pangrova::observability {

std::atomic<bool> NativeTracer::initialized_{false};

namespace {

/// Validate hex string: must be exactly expectedLen hex characters.
bool isValidHex(const std::string& hex, size_t expectedLen) {
    if (hex.size() != expectedLen) {
        return false;
    }
    for (char c : hex) {
        if (!((c >= '0' && c <= '9') ||
              (c >= 'a' && c <= 'f') ||
              (c >= 'A' && c <= 'F'))) {
            return false;
        }
    }
    return true;
}

#ifdef HAS_OTEL_CPP
// Module-level tracer instance, created during initialize().
opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> g_tracer;
#endif

} // namespace

void NativeTracer::initialize(const std::string& otlpEndpoint,
                               const std::string& serviceName) {
#ifdef HAS_OTEL_CPP
    try {
        // 1. Create OTLP gRPC exporter targeting the collector endpoint.
        opentelemetry::exporter::otlp::OtlpGrpcExporterOptions exporterOpts;
        exporterOpts.endpoint = otlpEndpoint;
        auto exporter =
            opentelemetry::exporter::otlp::OtlpGrpcExporterFactory::Create(
                exporterOpts);

        // 2. Create a BatchSpanProcessor for lower overhead in production.
        // SimpleSpanProcessor exports synchronously on each span end,
        // which adds latency to the hot path. BatchSpanProcessor buffers
        // spans and exports them in bulk on a background thread.
        auto processor =
            opentelemetry::sdk::trace::BatchSpanProcessorFactory::Create(
                std::move(exporter), {});

        // 3. Configure Resource with service.name attribute.
        auto resource = opentelemetry::sdk::resource::Resource::Create({
            {"service.name", serviceName},
            {"service.namespace", "pangrova"},
            {"service.component", "worker-native"},
        });

        // 4. Create TracerProvider and set as global.
        auto provider =
            opentelemetry::sdk::trace::TracerProviderFactory::Create(
                std::move(processor), resource);
        opentelemetry::trace::Provider::SetTracerProvider(
            opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(
                std::shared_ptr<opentelemetry::trace::TracerProvider>(std::move(provider))));

        // 5. Get a tracer instance for span creation.
        g_tracer = opentelemetry::trace::Provider::GetTracerProvider()
                       ->GetTracer("pangrova-native", "0.1.0");

        PANGROVA_LOG_INFO << "otel_native_tracer_initialized"
                        << " endpoint=" << otlpEndpoint
                        << " service_name=" << serviceName;
    } catch (const std::exception& e) {
        PANGROVA_LOG_WARN << "otel_native_tracer_init_failed"
                        << " endpoint=" << otlpEndpoint
                        << " error=" << e.what();
        // Fall through — initialized_ will still be set to true so
        // SpanScope stubs are created (graceful degradation).
    }
#else
    PANGROVA_LOG_INFO << "otel_native_tracer=stub"
                    << " reason=HAS_OTEL_CPP_not_defined"
                    << " endpoint=" << otlpEndpoint;
#endif
    initialized_.store(true, std::memory_order_release);
}

void NativeTracer::shutdown() {
#ifdef HAS_OTEL_CPP
    // Flush pending spans and clean up the TracerProvider.
    g_tracer = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>();
    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    if (provider) {
        // Cast to SDK TracerProvider to call ForceFlush/Shutdown.
        auto* sdkProvider = dynamic_cast<
            opentelemetry::sdk::trace::TracerProvider*>(provider.get());
        if (sdkProvider) {
            sdkProvider->ForceFlush();
            sdkProvider->Shutdown();
        }
    }
    PANGROVA_LOG_INFO << "otel_native_tracer_shutdown";
#endif
    initialized_.store(false, std::memory_order_release);
}

bool NativeTracer::isInitialized() {
    return initialized_.load(std::memory_order_acquire);
}

std::unique_ptr<SpanScope> NativeTracer::startSpanWithParent(
    const std::string& traceIdHex,
    const std::string& spanIdHex,
    const std::string& spanName) {
    if (!isInitialized()) {
        return nullptr;
    }

    // Validate trace ID (32 hex chars = 16 bytes) and span ID (16 hex chars = 8 bytes).
    // Empty spanIdHex is allowed (creates a root span with the given traceId).
    if (!isValidHex(traceIdHex, 32)) {
        return nullptr;
    }
    if (!spanIdHex.empty() && !isValidHex(spanIdHex, 16)) {
        return nullptr;
    }
    if (spanName.empty()) {
        return nullptr;
    }

    // Create child span linked to the Java-side parent context.
    return std::make_unique<SpanScope>(traceIdHex, spanIdHex, spanName);
}

std::unique_ptr<SpanScope> NativeTracer::startSpan(const std::string& spanName) {
    if (!isInitialized()) {
        return nullptr;
    }
    if (spanName.empty()) {
        return nullptr;
    }
    return std::make_unique<SpanScope>(spanName);
}

} // namespace pangrova::observability
