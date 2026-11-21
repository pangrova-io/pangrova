package org.pangrova.observability.sdk.tracing.otel;

import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;

/**
 * Post-hoc span processor that rescues traces which were not head-sampled
 * but match tail-based sampling criteria:
 *
 * <ul>
 *   <li><b>Error sampling:</b> any span ending with
 *       {@link io.opentelemetry.api.trace.StatusCode#ERROR} is exported</li>
 *   <li><b>Slow-query sampling:</b> any span whose duration exceeds a
 *       configurable threshold is exported</li>
 * </ul>
 *
 * <p>Head-sampled spans (those with {@code isSampled() == true}) are ignored
 * by this processor — they are already handled by the primary
 * {@link io.opentelemetry.sdk.trace.export.BatchSpanProcessor}.</p>
 *
 * <p><b>Design note:</b> for this processor to see non-head-sampled spans,
 * the head sampler must return {@code RECORD_ONLY} (not {@code DROP}) for
 * spans that fail the probability check. The companion
 * {@link #alwaysRecordSampler(io.opentelemetry.sdk.trace.samplers.Sampler)}
 * factory wraps any sampler to guarantee this.</p>
 *
 * <p>Tail-sampled spans are exported directly via the provided
 * {@link SpanExporter}. Since only error/slow spans qualify, volume is
 * bounded by the application's error/slow-query rate.</p>
 */
public final class TailBasedSpanProcessor implements SpanProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TailBasedSpanProcessor.class);

    private final SpanExporter exporter;
    private final boolean errorSamplingEnabled;
    private final long slowQueryThresholdNanos;

    /**
     * Creates a new tail-based span processor.
     *
     * @param exporter               the span exporter to use for tail-sampled spans
     * @param errorSamplingEnabled   if true, export spans that end with ERROR status
     * @param slowQueryThreshold     threshold duration; spans longer than this are exported.
     *                               {@link Duration#ZERO} disables slow-query sampling.
     */
    public TailBasedSpanProcessor(SpanExporter exporter,
                                  boolean errorSamplingEnabled,
                                  Duration slowQueryThreshold) {
        this.exporter = Objects.requireNonNull(exporter, "exporter");
        this.errorSamplingEnabled = errorSamplingEnabled;
        this.slowQueryThresholdNanos = slowQueryThreshold != null ? slowQueryThreshold.toNanos() : 0;
    }

    @Override
    public boolean isStartRequired() {
        return false;
    }

    @Override
    public void onStart(Context parentContext, ReadWriteSpan span) {
        // No-op: tail sampling evaluates at span end.
    }

    @Override
    public boolean isEndRequired() {
        return true;
    }

    @Override
    public void onEnd(ReadableSpan span) {
        if (span.getSpanContext().isSampled()) {
            // Already head-sampled — the BatchSpanProcessor will export this span.
            return;
        }

        SpanData data = span.toSpanData();

        if (shouldTailSample(data)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Tail-sampling span: name={}, traceId={}, reason={}",
                        data.getName(), data.getTraceId(), tailSampleReason(data));
            }
            exporter.export(Collections.singletonList(data));
        }
    }

    @Override
    public CompletableResultCode shutdown() {
        return exporter.shutdown();
    }

    @Override
    public CompletableResultCode forceFlush() {
        return exporter.flush();
    }

    private boolean shouldTailSample(SpanData data) {
        if (errorSamplingEnabled && isError(data)) {
            return true;
        }
        if (slowQueryThresholdNanos > 0 && exceedsDurationThreshold(data)) {
            return true;
        }
        return false;
    }

    private static boolean isError(SpanData data) {
        return data.getStatus().getStatusCode() == io.opentelemetry.api.trace.StatusCode.ERROR;
    }

    private boolean exceedsDurationThreshold(SpanData data) {
        long durationNanos = data.getEndEpochNanos() - data.getStartEpochNanos();
        return durationNanos >= slowQueryThresholdNanos;
    }

    private String tailSampleReason(SpanData data) {
        if (errorSamplingEnabled && isError(data)) {
            return "error";
        }
        return "slow_query";
    }

    /**
     * Wraps a delegate sampler so that {@code DROP} decisions become
     * {@code RECORD_ONLY}. This ensures all spans are at least recorded,
     * allowing tail-based evaluation at span end.
     *
     * <p>Spans that the delegate would have sampled ({@code RECORD_AND_SAMPLE})
     * are unchanged. Spans that would have been dropped are now recorded
     * (attributes and events are captured) but not exported by the primary
     * processor — only the tail-based processor considers them.</p>
     *
     * @param delegate the original head-based sampler
     * @return a sampler that never drops spans
     */
    public static io.opentelemetry.sdk.trace.samplers.Sampler alwaysRecordSampler(
            io.opentelemetry.sdk.trace.samplers.Sampler delegate) {
        return new AlwaysRecordSampler(delegate);
    }

    /**
     * Sampler wrapper that converts DROP to RECORD_ONLY so that tail-based
     * evaluation can inspect every span.
     */
    private static final class AlwaysRecordSampler implements io.opentelemetry.sdk.trace.samplers.Sampler {

        private final io.opentelemetry.sdk.trace.samplers.Sampler delegate;

        AlwaysRecordSampler(io.opentelemetry.sdk.trace.samplers.Sampler delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public io.opentelemetry.sdk.trace.samplers.SamplingResult shouldSample(
                Context parentContext,
                String traceId,
                String name,
                io.opentelemetry.api.trace.SpanKind spanKind,
                io.opentelemetry.api.common.Attributes attributes,
                java.util.List<io.opentelemetry.sdk.trace.data.LinkData> parentLinks) {

            io.opentelemetry.sdk.trace.samplers.SamplingResult result =
                    delegate.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);

            if (result.getDecision() == io.opentelemetry.sdk.trace.samplers.SamplingDecision.DROP) {
                return io.opentelemetry.sdk.trace.samplers.SamplingResult.recordOnly();
            }
            return result;
        }

        @Override
        public String getDescription() {
            return "AlwaysRecordSampler{delegate=" + delegate.getDescription() + "}";
        }
    }
}
