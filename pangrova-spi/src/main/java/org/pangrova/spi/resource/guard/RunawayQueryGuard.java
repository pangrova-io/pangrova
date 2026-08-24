package org.pangrova.spi.resource.guard;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;
import java.util.Optional;

/**
 * SPI for per-query runaway detection and noisy-neighbour defence.
 *
 * <p>A runaway query guard evaluates queries' resource consumption against
 * configurable thresholds (CPU time, scanned bytes/rows, memory) and
 * returns a {@link GuardVerdict} when a query exceeds its limits. The
 * verdict indicates the recommended action: {@link GuardAction#KILL},
 * {@link GuardAction#THROTTLE}, or {@link GuardAction#WARN}.</p>
 *
 * <p>This SPI addresses gap G-RES-06: Pangrova had no per-query big-query
 * guard and no memory-based query killer. The existing
 * {@code CriticalPressureHandler} is a reactive last-resort (kills the
 * largest consumer only once the node is in CRITICAL pressure); this SPI
 * provides the <em>proactive</em> per-query guard (StarRocks "big query"
 * model) that should run periodically on a scheduler thread.</p>
 *
 * <p>The guard is a pure decision function: it does not cancel queries
 * itself. The execution layer is responsible for acting on verdicts
 * (e.g. calling {@code TaskManager.cancelTask}).</p>
 *
 * <p>Implementations must be thread-safe.</p>
 *
 * @see QueryResourceSnapshot
 * @see GuardVerdict
 * @see GuardAction
 */
@Experimental(reason = "G-RES-06: runaway query guard SPI "
        + "-- not yet wired to real-time execution path or native CPU/scan counters")
public interface RunawayQueryGuard {

    /**
     * Evaluates a single query against the configured thresholds.
     *
     * @param snapshot the query's current resource consumption
     * @return a verdict if the query has breached any threshold, or empty if within limits
     */
    Optional<GuardVerdict> evaluate(QueryResourceSnapshot snapshot);

    /**
     * Evaluates a batch of queries and returns verdicts for the violators.
     *
     * <p>The default implementation delegates to {@link #evaluate} per query.
     * Implementations may override for batch-optimised evaluation.</p>
     *
     * @param snapshots the queries' current resource consumption
     * @return the verdicts for queries that breached thresholds, in input order; empty if all are within limits
     */
    default List<GuardVerdict> evaluateBatch(List<QueryResourceSnapshot> snapshots) {
        return snapshots.stream()
                .map(this::evaluate)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
    }

    /**
     * Returns the human-readable name of this guard
     * (e.g. "threshold-based", "ml-anomaly", "adaptive").
     *
     * @return guard name, never null or blank
     */
    String name();
}
