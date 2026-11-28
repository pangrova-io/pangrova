package org.pangrova.worker.resource.memory;

import org.pangrova.worker.enums.NativeMemoryEventType;
import org.pangrova.worker.exception.NativeQuotaSendException;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.worker.resource.tenant.TenantResourceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Heartbeat-based implementation of cross-domain memory coordination.
 *
 * <p>Maintains a per-tenant native usage ledger updated by periodic
 * {@link NativeMemoryEvent} heartbeats. Provides combined (Java + Native)
 * usage for arbitration decisions.
 *
 * <p>Concurrency strategy: per-tenant state uses {@link AtomicLong} for
 * allocation tracking and volatile timestamps for staleness detection.
 * No locks are held during I/O (quota push is fire-and-forget with error logging).
 *
 * <p>Failure mode: if no heartbeat is received within {@code heartbeatTimeoutMillis},
 * the tenant's native stat is considered stale. {@link #combinedUsage} degrades
 * to Java-only view and increments the stale counter metric.
 */
@ThreadSafe
public final class HeartbeatCrossDomainMemoryCoordinator implements CrossDomainMemoryCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatCrossDomainMemoryCoordinator.class);

    /** Default heartbeat timeout: 10 seconds. */
    private static final long DEFAULT_HEARTBEAT_TIMEOUT_MILLIS = 10_000;

    private final TenantResourceTracker tenantTracker;
    private final NativeQuotaSender nativeQuotaSender;
    private final Clock clock;
    private final long heartbeatTimeoutMillis;

    /** Per-tenant native allocation bytes (updated on each NativeMemoryEvent). */
    private final ConcurrentHashMap<String, AtomicLong> nativeUsage = new ConcurrentHashMap<>();

    /** Per-tenant last heartbeat timestamp (epoch millis). */
    private final ConcurrentHashMap<String, AtomicLong> lastHeartbeat = new ConcurrentHashMap<>();

    /** Counter for stale native stat occurrences (observable via metric). */
    private final AtomicLong nativeStatStaleTotal = new AtomicLong(0);

    /**
     * Creates a coordinator with all dependencies injected.
     *
     * @param tenantTracker         per-tenant Java-side resource tracker
     * @param nativeQuotaSender     sender for pushing quota changes to native
     * @param clock                 clock for staleness detection (supports testing)
     * @param heartbeatTimeoutMillis max millis between heartbeats before degraded mode
     */
    public HeartbeatCrossDomainMemoryCoordinator(
            TenantResourceTracker tenantTracker,
            NativeQuotaSender nativeQuotaSender,
            Clock clock,
            long heartbeatTimeoutMillis) {
        if (tenantTracker == null) {
            throw new IllegalArgumentException("tenantTracker must not be null");
        }
        if (nativeQuotaSender == null) {
            throw new IllegalArgumentException("nativeQuotaSender must not be null");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock must not be null");
        }
        if (heartbeatTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "heartbeatTimeoutMillis must be positive, got " + heartbeatTimeoutMillis);
        }
        this.tenantTracker = tenantTracker;
        this.nativeQuotaSender = nativeQuotaSender;
        this.clock = clock;
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
    }

    /**
     * Creates a coordinator with default heartbeat timeout (10s) and system clock.
     */
    public HeartbeatCrossDomainMemoryCoordinator(
            TenantResourceTracker tenantTracker,
            NativeQuotaSender nativeQuotaSender) {
        this(tenantTracker, nativeQuotaSender, Clock.systemUTC(), DEFAULT_HEARTBEAT_TIMEOUT_MILLIS);
    }

    @Override
    public void publishQuotaChange(String tenantId, long bytes) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be null or blank");
        }
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be positive, got " + bytes);
        }
        try {
            nativeQuotaSender.setTenantQuota(tenantId, bytes);
            LOG.info("event=quota_pushed_to_native tenantId={} bytes={}", tenantId, bytes);
        } catch (NativeQuotaSendException e) {
            LOG.error("event=quota_push_failed tenantId={} bytes={} error={}",
                    tenantId, bytes, e.getMessage());
            throw e;
        }
    }

    @Override
    public void onNativeEvent(NativeMemoryEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        String tenantId = event.tenantId();
        nativeUsage.computeIfAbsent(tenantId, k -> new AtomicLong(0))
                .set(event.allocatedBytes());
        lastHeartbeat.computeIfAbsent(tenantId, k -> new AtomicLong(0))
                .set(event.timestampMillis());

        if (event.type() != NativeMemoryEventType.STAT) {
            LOG.info("event=native_memory_event tenantId={} type={} allocatedBytes={}",
                    tenantId, event.type(), event.allocatedBytes());
        } else {
            LOG.debug("event=native_memory_stat tenantId={} allocatedBytes={}",
                    tenantId, event.allocatedBytes());
        }
    }

    @Override
    public long combinedUsage(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be null or blank");
        }
        long javaUsage = tenantTracker.getMemoryUsage(tenantId);
        if (isNativeStatStale(tenantId)) {
            nativeStatStaleTotal.incrementAndGet();
            LOG.warn("event=native_stat_stale tenantId={} javaUsage={} degraded=java_only",
                    tenantId, javaUsage);
            return javaUsage;
        }
        AtomicLong nativeBytes = nativeUsage.get(tenantId);
        long nativeAlloc = nativeBytes != null ? nativeBytes.get() : 0;
        return javaUsage + nativeAlloc;
    }

    @Override
    public boolean isNativeStatStale(String tenantId) {
        AtomicLong lastTs = lastHeartbeat.get(tenantId);
        if (lastTs == null) {
            return true;
        }
        long elapsed = clock.millis() - lastTs.get();
        return elapsed > heartbeatTimeoutMillis;
    }

    /**
     * Returns the native-only allocation for a tenant (latest reported value).
     * Returns 0 if no native event has been received for this tenant.
     */
    public long nativeUsage(String tenantId) {
        AtomicLong usage = nativeUsage.get(tenantId);
        return usage != null ? usage.get() : 0;
    }

    /**
     * Returns the total count of stale native stat occurrences.
     * Intended for metric export (native_stat_stale_total).
     */
    public long staleStatCount() {
        return nativeStatStaleTotal.get();
    }
}
