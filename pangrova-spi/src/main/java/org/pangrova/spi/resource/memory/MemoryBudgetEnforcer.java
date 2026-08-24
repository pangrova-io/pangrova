package org.pangrova.spi.resource.memory;

import org.pangrova.spi.annotation.Experimental;

import java.util.Map;

/**
 * SPI for unified memory budget enforcement across Java heap, native,
 * and off-heap domains.
 *
 * <p>This SPI addresses gap G-RES-07: Pangrova's Java and native arbitrators
 * were not unified (the shared path was off by default, the setter was never
 * called), so a true cross-domain memory budget was not enforced. Dynamic
 * levers were inert: {@code NativeMemoryCoordinator.shrink} only logged and
 * returned 0, and {@code SpillManager} faked reclamation.</p>
 *
 * <p>A {@code MemoryBudgetEnforcer} is fed cross-domain snapshots
 * ({@link MemoryDomainSnapshot} per domain) and produces an
 * {@link EnforcementDecision} that the worker resource bootstrap acts on.
 * The enforcer is a <em>pure decision function</em>; the execution of
 * spill/shrink/kill is the caller's responsibility.</p>
 *
 * <h3>Responsibilities</h3>
 * <ul>
 *   <li>Evaluate global pressure from merged cross-domain usage.</li>
 *   <li>Decide which lever to pull: SPILL, SHRINK_NATIVE, RECLAIM,
 *       or KILL_LARGEST.</li>
 *   <li>Expose a merged cross-domain gauge for Prometheus/OTel
 *       (via {@link #mergedUtilisation}).</li>
 *   <li>Detect drift between domain budgets and actual usage
 *       (via {@link #detectDrift}).</li>
 * </ul>
 *
 * <p>Implementations must be thread-safe.</p>
 *
 * @see MemoryDomainSnapshot
 * @see EnforcementDecision
 * @see EnforcementAction
 */
@Experimental(reason = "G-RES-07: unified memory budget enforcement SPI "
        + "-- NativeMemoryCoordinator.shrink still inert, no cross-domain gauge yet")
public interface MemoryBudgetEnforcer {

    /**
     * Evaluates the cross-domain memory snapshots and decides the enforcement action.
     *
     * @param snapshots per-domain memory snapshots (keyed by domain); never null or empty
     * @return the enforcement decision (never null)
     */
    EnforcementDecision enforce(Map<MemoryDomain, MemoryDomainSnapshot> snapshots);

    /**
     * Computes the merged cross-domain memory utilisation ratio.
     *
     * <p>This is the value that should be exposed as a Prometheus/OTel gauge
     * for unified memory monitoring.</p>
     *
     * @param snapshots per-domain memory snapshots
     * @return merged utilisation ratio (0.0 = idle, 1.0 = at budget, > 1.0 = over-committed)
     */
    double mergedUtilisation(Map<MemoryDomain, MemoryDomainSnapshot> snapshots);

    /**
     * Detects budget drift: whether the sum of per-domain budgets exceeds the
     * physical memory capacity or whether actual usage has drifted significantly
     * from accounting.
     *
     * <p>Implementations should return an empty string when no drift is detected,
     * or a human-readable drift description suitable for alerting.</p>
     *
     * @param snapshots per-domain memory snapshots
     * @param physicalMemoryBytes total physical memory available on the node
     * @return drift description, or empty string if no drift
     */
    String detectDrift(Map<MemoryDomain, MemoryDomainSnapshot> snapshots, long physicalMemoryBytes);

    /**
     * Returns the human-readable name of this enforcer
     * (e.g. "threshold-based", "proportional", "adaptive").
     *
     * @return enforcer name, never null or blank
     */
    String name();
}
