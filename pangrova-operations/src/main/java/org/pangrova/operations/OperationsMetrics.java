package org.pangrova.operations;

import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;

import java.util.List;
import java.util.Objects;

/**
 * Operational metrics required by documentation section 10.
 *
 * <p>Registers and exposes the following instruments:
 * <ul>
 *   <li>{@code pangrova.operations.ddl.duration} - DDL execution duration histogram (seconds)</li>
 *   <li>{@code pangrova.operations.scaling.events} - Count of scaling events (scale-out/scale-in)</li>
 *   <li>{@code pangrova.operations.config.reloads} - Count of successful configuration reloads</li>
 *   <li>{@code pangrova.operations.bootstrap.phase.duration} - Bootstrap phase duration histogram (seconds)</li>
 * </ul>
 *
 * <p>This class is instantiated with a {@link MetricsProvider} and passed into operational
 * components that need to emit metrics. When no real metrics backend is configured, the
 * NoOp provider from {@code pangrova-observability} is used, making instrumentation zero-cost.
 */
public final class OperationsMetrics {

    private static final List<Double> DURATION_BOUNDARIES_S =
            List.of(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0);

    private final DoubleHistogram ddlDuration;
    private final LongCounter scalingEvents;
    private final LongCounter configReloads;
    private final DoubleHistogram bootstrapPhaseDuration;

    /** Creates all operational metric instruments from the given provider. */
    public OperationsMetrics(MetricsProvider provider) {
        Objects.requireNonNull(provider, "provider");
        this.ddlDuration = provider.createHistogram(
                "pangrova.operations.ddl.duration",
                "Duration of DDL operations",
                "s",
                DURATION_BOUNDARIES_S);
        this.scalingEvents = provider.createCounter(
                "pangrova.operations.scaling.events",
                "Number of scaling events triggered",
                "{event}");
        this.configReloads = provider.createCounter(
                "pangrova.operations.config.reloads",
                "Number of successful configuration reloads",
                "{reload}");
        this.bootstrapPhaseDuration = provider.createHistogram(
                "pangrova.operations.bootstrap.phase.duration",
                "Duration of bootstrap phases",
                "s",
                DURATION_BOUNDARIES_S);
    }

    /**
     * Records the duration of a DDL operation.
     *
     * @param durationSeconds elapsed time in seconds
     * @param ddlType         the DDL type (METADATA_ONLY, DATA_REWRITE, TABLE_LOCK)
     * @param outcome         the outcome (completed, failed, timed_out, cancelled)
     */
    public void recordDdlDuration(double durationSeconds, String ddlType, String outcome) {
        MetricAttributes attrs = MetricAttributes.of("ddl.type", ddlType, "outcome", outcome);
        ddlDuration.record(durationSeconds, attrs);
    }

    /**
     * Records a scaling event.
     *
     * @param direction the scaling direction ("scale_out" or "scale_in")
     */
    public void recordScalingEvent(String direction) {
        MetricAttributes attrs = MetricAttributes.of("direction", direction);
        scalingEvents.add(1, attrs);
    }

    /**
     * Records a successful configuration reload.
     */
    public void recordConfigReload() {
        configReloads.add(1);
    }

    /**
     * Records the duration of a bootstrap phase.
     *
     * @param durationSeconds elapsed time in seconds
     * @param phase           the bootstrap phase name
     */
    public void recordBootstrapPhaseDuration(double durationSeconds, String phase) {
        MetricAttributes attrs = MetricAttributes.of("phase", phase);
        bootstrapPhaseDuration.record(durationSeconds, attrs);
    }

    // Accessors for testing

    DoubleHistogram getDdlDuration() {
        return ddlDuration;
    }

    LongCounter getScalingEvents() {
        return scalingEvents;
    }

    LongCounter getConfigReloads() {
        return configReloads;
    }

    DoubleHistogram getBootstrapPhaseDuration() {
        return bootstrapPhaseDuration;
    }
}
