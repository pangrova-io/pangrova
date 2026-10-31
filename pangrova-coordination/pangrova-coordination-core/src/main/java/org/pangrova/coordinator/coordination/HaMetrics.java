package org.pangrova.coordinator.coordination;

import org.pangrova.observability.api.LongCounter;
import org.pangrova.observability.api.MetricAttributes;
import org.pangrova.observability.api.MetricsProvider;
import org.pangrova.observability.api.ObservableDoubleGauge;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Metrics instrumentation for the HA critical path.
 *
 * <p>Exposes the following instruments:
 * <ul>
 *   <li>{@code pangrova.coordinator.ha.role} — gauge: 1.0 = LEADER, 0.0 = STANDBY/STOPPED</li>
 *   <li>{@code pangrova.coordinator.ha.epoch} — gauge: current epoch value</li>
 *   <li>{@code pangrova.coordinator.ha.failovers} — counter: total failover events</li>
 *   <li>{@code pangrova.coordinator.ha.renewal.failures} — counter: lease renewal failures</li>
 *   <li>{@code pangrova.coordinator.ha.recovery} — counter with {@code result} attribute (success/failure)</li>
 * </ul>
 *
 * <p>Fixes finding F-COORD-HA-08: HA critical path had no observability metrics.</p>
 */
public final class HaMetrics {

    private final ObservableDoubleGauge roleGauge;
    private final ObservableDoubleGauge epochGauge;
    private final LongCounter failoversTotal;
    private final LongCounter renewalFailuresTotal;
    private final LongCounter recovery;

    private static final MetricAttributes RESULT_SUCCESS = MetricAttributes.of("result", "success");
    private static final MetricAttributes RESULT_FAILURE = MetricAttributes.of("result", "failure");

    /**
     * Creates HA metrics backed by the given provider, reading live state from the HA service.
     *
     * @param provider      the metrics provider (NoOp-safe)
     * @param roleSupplier  supplies the current role as a double (1.0=LEADER, 0.0=other)
     * @param epochSupplier supplies the current epoch as a double
     */
    public HaMetrics(MetricsProvider provider,
                     Supplier<Double> roleSupplier,
                     Supplier<Double> epochSupplier) {
        Objects.requireNonNull(provider, "provider must not be null");
        Objects.requireNonNull(roleSupplier, "roleSupplier must not be null");
        Objects.requireNonNull(epochSupplier, "epochSupplier must not be null");

        this.roleGauge = provider.createGauge(
                "pangrova.coordinator.ha.role",
                "Current HA role (1=leader, 0=standby/stopped)",
                "{role}",
                roleSupplier);

        this.epochGauge = provider.createGauge(
                "pangrova.coordinator.ha.epoch",
                "Current HA epoch value",
                "{epoch}",
                epochSupplier);

        this.failoversTotal = provider.createCounter(
                "pangrova.coordinator.ha.failovers",
                "Total number of leader failover events",
                "{failover}");

        this.renewalFailuresTotal = provider.createCounter(
                "pangrova.coordinator.ha.renewal.failures",
                "Total number of lease renewal failures",
                "{failure}");

        this.recovery = provider.createCounter(
                "pangrova.coordinator.ha.recovery",
                "Recovery attempts with result tag",
                "{recovery}");
    }

    /** Records a failover event (new leader elected). */
    public void recordFailover() {
        failoversTotal.add(1);
    }

    /** Records a lease renewal failure. */
    public void recordRenewalFailure() {
        renewalFailuresTotal.add(1);
    }

    /** Records a successful recovery. */
    public void recordRecoverySuccess() {
        recovery.add(1, RESULT_SUCCESS);
    }

    /** Records a failed recovery. */
    public void recordRecoveryFailure() {
        recovery.add(1, RESULT_FAILURE);
    }
}
