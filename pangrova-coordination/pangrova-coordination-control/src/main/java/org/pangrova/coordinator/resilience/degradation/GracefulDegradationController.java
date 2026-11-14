package org.pangrova.coordinator.resilience.degradation;

import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Controls graceful degradation under resource pressure.
 *
 * <p>When the system is under pressure (memory, CPU, or queue depth exceeding thresholds),
 * this controller:
 * <ul>
 *   <li>Rejects new queries with a backpressure signal</li>
 *   <li>Prioritizes already-running queries over new admissions</li>
 *   <li>Sheds load by tenant priority (lower-priority tenants are rejected first)</li>
 * </ul>
 */
public final class GracefulDegradationController {

    private static final Logger LOG = LoggerFactory.getLogger(GracefulDegradationController.class);

    private final double memoryPressureThreshold;
    private final double cpuPressureThreshold;
    private final int maxQueueDepth;

    /**
     * Creates a degradation controller with the given pressure thresholds.
     *
     * @param memoryPressureThreshold memory usage ratio (0.0-1.0) above which degradation activates
     * @param cpuPressureThreshold    CPU usage ratio (0.0-1.0) above which degradation activates
     * @param maxQueueDepth           maximum pending query queue depth before rejection
     */
    public GracefulDegradationController(double memoryPressureThreshold,
                                         double cpuPressureThreshold,
                                         int maxQueueDepth) {
        if (memoryPressureThreshold <= 0.0 || memoryPressureThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "memoryPressureThreshold must be in (0.0, 1.0], got: " + memoryPressureThreshold);
        }
        if (cpuPressureThreshold <= 0.0 || cpuPressureThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "cpuPressureThreshold must be in (0.0, 1.0], got: " + cpuPressureThreshold);
        }
        if (maxQueueDepth <= 0) {
            throw new IllegalArgumentException("maxQueueDepth must be positive, got: " + maxQueueDepth);
        }
        this.memoryPressureThreshold = memoryPressureThreshold;
        this.cpuPressureThreshold = cpuPressureThreshold;
        this.maxQueueDepth = maxQueueDepth;
    }

    /**
     * Evaluates whether a new query should be admitted given current system pressure.
     *
     * @param request the admission request containing tenant priority and system state
     * @return the admission decision with reason
     */
    public AdmissionDecision evaluateAdmission(AdmissionRequest request) {
        if (request.currentQueueDepth() >= maxQueueDepth) {
            LOG.warn("Query rejected: queueDepth={} maxQueueDepth={} tenant={} priority={}",
                    request.currentQueueDepth(), maxQueueDepth,
                    request.tenantId(), request.tenantPriority());
            return new AdmissionDecision(false, RejectionReason.QUEUE_FULL);
        }

        if (request.memoryUsageRatio() >= memoryPressureThreshold) {
            LOG.warn("Query rejected: memoryUsage={} threshold={} tenant={} priority={}",
                    request.memoryUsageRatio(), memoryPressureThreshold,
                    request.tenantId(), request.tenantPriority());
            return new AdmissionDecision(false, RejectionReason.MEMORY_PRESSURE);
        }

        if (request.cpuUsageRatio() >= cpuPressureThreshold) {
            LOG.warn("Query rejected: cpuUsage={} threshold={} tenant={} priority={}",
                    request.cpuUsageRatio(), cpuPressureThreshold,
                    request.tenantId(), request.tenantPriority());
            return new AdmissionDecision(false, RejectionReason.CPU_PRESSURE);
        }

        return new AdmissionDecision(true, null);
    }

    /**
     * Determines which queries to shed when the system is under sustained pressure.
     * Queries from lower-priority tenants are shed first; running queries are preserved
     * over pending queries.
     *
     * @param pendingQueries the list of pending (not yet started) queries
     * @return queries to shed, ordered from lowest to highest priority
     */
    public List<QueryLoadEntry> selectQueriesToShed(List<QueryLoadEntry> pendingQueries) {
        if (pendingQueries == null || pendingQueries.isEmpty()) {
            return List.of();
        }
        return pendingQueries.stream()
                .sorted(Comparator.comparingInt(QueryLoadEntry::tenantPriority))
                .toList();
    }

    /**
     * Returns the configured memory pressure threshold.
     */
    public double getMemoryPressureThreshold() {
        return memoryPressureThreshold;
    }

    /**
     * Returns the configured CPU pressure threshold.
     */
    public double getCpuPressureThreshold() {
        return cpuPressureThreshold;
    }

    /**
     * Returns the configured maximum queue depth.
     */
    public int getMaxQueueDepth() {
        return maxQueueDepth;
    }

    /**
     * Request for query admission evaluation.
     *
     * @param tenantId         the tenant submitting the query
     * @param tenantPriority   the tenant's priority level (higher = more important)
     * @param memoryUsageRatio current system memory usage ratio (0.0-1.0)
     * @param cpuUsageRatio    current system CPU usage ratio (0.0-1.0)
     * @param currentQueueDepth current number of pending queries in the queue
     */
    public record AdmissionRequest(
            String tenantId,
            int tenantPriority,
            double memoryUsageRatio,
            double cpuUsageRatio,
            int currentQueueDepth
    ) {
        public AdmissionRequest {
            if (tenantId == null || tenantId.isBlank()) {
                throw new IllegalArgumentException("tenantId must not be blank");
            }
        }
    }

    /**
     * Result of an admission evaluation.
     *
     * @param admitted whether the query is admitted
     * @param reason   the rejection reason (null if admitted)
     */
    public record AdmissionDecision(boolean admitted, RejectionReason reason) {
    }

    /**
     * Entry representing a pending query for load shedding decisions.
     *
     * @param queryId        the query identifier
     * @param tenantId       the tenant that owns the query
     * @param tenantPriority the tenant's priority level (higher = more important)
     */
    public record QueryLoadEntry(String queryId, String tenantId, int tenantPriority) {
        public QueryLoadEntry {
            if (queryId == null || queryId.isBlank()) {
                throw new IllegalArgumentException("queryId must not be blank");
            }
            if (tenantId == null || tenantId.isBlank()) {
                throw new IllegalArgumentException("tenantId must not be blank");
            }
        }
    }

    /**
     * Reasons for rejecting a query admission.
     */
    public enum RejectionReason {
        MEMORY_PRESSURE,
        CPU_PRESSURE,
        QUEUE_FULL
    }
}
