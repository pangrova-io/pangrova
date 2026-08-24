package org.pangrova.spi.resource.scaling;

import org.pangrova.spi.annotation.Experimental;

/**
 * SPI for cluster auto-scaling decisions.
 *
 * <p>An {@code AutoScaler} evaluates cluster-wide metrics and produces a
 * {@link ScalingDecision} that the orchestration layer executes (e.g. via
 * Kubernetes replica adjustment, cloud API calls, or manual worker lifecycle).
 * The auto-scaler is a <em>pure decision function</em>: it does not execute
 * scaling itself; that responsibility belongs to the scaling executor.</p>
 *
 * <p>The contract covers three elastic behaviours previously missing from Pangrova
 * (G-RES-03):</p>
 * <ol>
 *   <li><b>Scale-out</b> -- triggered by high queue depth, CPU/memory pressure,
 *       or emergency thresholds (bypasses cooldown).</li>
 *   <li><b>Scale-in</b> -- triggered when load drops below a sustainable floor.</li>
 *   <li><b>Auto-suspend</b> -- sustained idle period (zero queue, low composite
 *       score) shrinks the pool to a configured minimum
 *       (Snowflake warehouse auto-suspend / Databricks SQL idle scale-down).</li>
 * </ol>
 *
 * <p>Implementations must be thread-safe; the control loop may invoke
 * {@link #evaluate} from a periodic scheduler thread.</p>
 *
 * @see ScalingDecision
 * @see ClusterMetricsSnapshot
 */
@Experimental(reason = "G-RES-03: auto-scaling SPI — control loop not yet wired to production metric sources")
public interface AutoScaler {

    /**
     * Evaluates the current cluster metrics and decides the next scaling action.
     *
     * @param metrics a point-in-time snapshot of cluster-wide resource metrics
     * @return the scaling decision (never null)
     */
    ScalingDecision evaluate(ClusterMetricsSnapshot metrics);

    /**
     * Returns the human-readable name of this auto-scaler strategy
     * (e.g. "threshold-based", "predictive", "rule-based").
     *
     * @return strategy name, never null or blank
     */
    String name();
}
