package org.pangrova.security.tenant.quota;

import org.pangrova.security.tenant.model.ResourceQuota;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.security.tenant.store.TenantStore;
import org.pangrova.spi.resource.admission.AdmissionDecision;
import org.pangrova.spi.resource.admission.AdmissionRequest;
import org.pangrova.spi.resource.admission.ClusterAdmissionController;
import org.pangrova.spi.resource.tenant.GlobalTenantUsageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ClusterAdmissionController} that enforces tenant resource quotas at
 * the coordinator admission gate.
 *
 * <p>Resolves G-RES-01: previously, quotas were never loaded into any tracker
 * ({@code .setQuota()} had zero call sites) and {@code TaskMemoryRequestor}
 * did not enforce tenant-level limits. This controller loads quotas from
 * {@link TenantStore} and atomically tracks per-tenant admitted query counts
 * using CAS, eliminating the TOCTOU race in the earlier check-then-act
 * pattern.</p>
 *
 * <p>Concurrency model: per-tenant {@link AtomicInteger} counters in a
 * {@link ConcurrentHashMap}, incremented atomically on admit and decremented
 * on release. Memory-aware admission is supported when the
 * {@link AdmissionRequest} carries a CBO estimate.</p>
 *
 * <p>Benchmarked against StarRocks resource group (mem_limit + concurrency).</p>
 */
public final class TenantQuotaAdmissionController implements ClusterAdmissionController {

    private static final Logger LOG = LoggerFactory.getLogger(TenantQuotaAdmissionController.class);

    private final TenantStore tenantStore;
    private final QuotaChecker quotaChecker;

    /** Optional cluster-wide usage view fed by worker heartbeats (P1-5); null = local-only. */
    private final GlobalTenantUsageView globalUsageView;

    /**
     * A-ADMISSION-01: optional ATOMIC cluster-wide concurrency gate. When non-null it is the
     * authoritative admission gate (no lagged-view TOCTOU oversell). Null = local counters only.
     */
    private final ClusterConcurrencySlots clusterSlots;

    /** Per-tenant admitted (in-flight) query counts. */
    private final ConcurrentHashMap<String, AtomicInteger> admittedCounts = new ConcurrentHashMap<>();

    /** Per-tenant queued query counts. */
    private final ConcurrentHashMap<String, AtomicInteger> queuedCounts = new ConcurrentHashMap<>();

    /** Tracks which queries were queued (queryId → tenantId). Only queued queries are tracked here. */
    private final ConcurrentMap<String, String> queuedQueries = new ConcurrentHashMap<>();

    public TenantQuotaAdmissionController(TenantStore tenantStore, QuotaChecker quotaChecker) {
        this(tenantStore, quotaChecker, null, null);
    }

    /**
     * Creates a controller that additionally enforces cluster-wide limits using a
     * heartbeat-fed {@link GlobalTenantUsageView}. When {@code globalUsageView} is
     * null the controller enforces only this coordinator's local counters
     * (single-node semantics), preserving backward-compatible behavior.
     */
    public TenantQuotaAdmissionController(TenantStore tenantStore, QuotaChecker quotaChecker,
                                          GlobalTenantUsageView globalUsageView) {
        this(tenantStore, quotaChecker, globalUsageView, null);
    }

    /**
     * A-ADMISSION-01: creates a controller whose concurrency gate is an ATOMIC cluster-wide
     * reservation ({@code clusterSlots}) shared by all coordinators, eliminating the oversell that
     * the heartbeat-fed (lagged) view permits. When {@code clusterSlots} is null, falls back to the
     * local counter (+ optional lagged view) path.
     */
    public TenantQuotaAdmissionController(TenantStore tenantStore, QuotaChecker quotaChecker,
                                          GlobalTenantUsageView globalUsageView,
                                          ClusterConcurrencySlots clusterSlots) {
        this.tenantStore = Objects.requireNonNull(tenantStore, "tenantStore must not be null");
        this.quotaChecker = Objects.requireNonNull(quotaChecker, "quotaChecker must not be null");
        this.globalUsageView = globalUsageView;
        this.clusterSlots = clusterSlots;
    }

    @Override
    public AdmissionDecision admit(AdmissionRequest request) {
        String tenantId = request.tenantId();

        Tenant tenant = tenantStore.getTenant(tenantId).orElse(null);
        AdmissionDecision tenantRejection = rejectInactiveTenant(tenantId, tenant);
        if (tenantRejection != null) {
            return tenantRejection;
        }

        ResourceQuota quota = tenant.quota();
        if (quota == null) {
            incrementAdmitted(tenantId);
            return AdmissionDecision.admit();
        }

        AtomicInteger counter = admittedCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0));
        AtomicInteger queueCounter = queuedCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0));

        // A-ADMISSION-01: with a distributed slot store, use an ATOMIC cluster-wide reservation as
        // the authoritative concurrency gate — eliminating the multi-coordinator oversell that the
        // heartbeat-fed (lagged) GlobalTenantUsageView allows via read-then-admit TOCTOU.
        if (clusterSlots != null) {
            return admitViaClusterSlots(request, quota, counter, queueCounter, tenantId);
        }
        return admitViaLocalCounters(request, quota, counter, queueCounter, tenantId);
    }

    /**
     * Admission gated by this coordinator's local counter, optionally pre-checked against the
     * heartbeat-fed cluster view. Extracted from {@link #admit} to keep both methods within the
     * project cyclomatic-complexity budget, and to mirror {@link #admitViaClusterSlots}.
     */
    private AdmissionDecision admitViaLocalCounters(AdmissionRequest request, ResourceQuota quota,
            AtomicInteger counter, AtomicInteger queueCounter, String tenantId) {
        // P1-5 phase 2: cluster-wide concurrency backpressure. The heartbeat-fed
        // global view reflects tenant queries active across ALL nodes; when the
        // cluster is already at the concurrency limit, queue/reject even if this
        // coordinator's local count is low, preventing multi-coordinator oversell.
        GlobalTenantUsageView view = this.globalUsageView;
        if (view != null) {
            int clusterActive = view.aggregatedActiveQueries(tenantId);
            if (clusterActive >= quota.maxConcurrentQueries()) {
                return queueOrReject(request, quota, queueCounter, tenantId, clusterActive);
            }
        }

        // CAS loop: atomically check against concurrent query limit
        while (true) {
            int current = counter.get();
            if (current >= quota.maxConcurrentQueries()) {
                return queueOrReject(request, quota, queueCounter, tenantId, current);
            }
            if (counter.compareAndSet(current, current + 1)) {
                return checkMemoryAndAdmit(request, quota, counter, tenantId, current);
            }
        }
    }

    private AdmissionDecision rejectInactiveTenant(String tenantId, Tenant tenant) {
        if (tenant == null) {
            LOG.warn("event=admission_rejected tenantId={} reason=tenant_not_found", tenantId);
            return AdmissionDecision.reject("tenant not found: " + tenantId);
        }
        if (tenant.status() != TenantStatus.ACTIVE) {
            LOG.warn("event=admission_rejected tenantId={} reason=tenant_{}", tenantId, tenant.status());
            return AdmissionDecision.reject("tenant is " + tenant.status() + ": " + tenantId);
        }
        return null;
    }

    /** Handles admission when at concurrency limit: queue if possible, otherwise reject. */
    private AdmissionDecision queueOrReject(AdmissionRequest request, ResourceQuota quota,
                                            AtomicInteger queueCounter, String tenantId, int current) {
        int queued = queueCounter.get();
        if (queued >= quota.maxQueuedQueries()) {
            LOG.info("event=admission_rejected tenantId={} queryId={} "
                            + "concurrent={} max={} queued={} maxQueued={}",
                    tenantId, request.queryId(), current,
                    quota.maxConcurrentQueries(), queued, quota.maxQueuedQueries());
            return AdmissionDecision.reject(
                    "concurrent query limit reached (" + current + "/"
                        + quota.maxConcurrentQueries() + ") and queue full ("
                        + queued + "/" + quota.maxQueuedQueries() + ")");
        }
        queueCounter.incrementAndGet();
        queuedQueries.put(request.queryId(), tenantId);
        LOG.debug("event=admission_queued tenantId={} queryId={} concurrent={} queued={}",
                tenantId, request.queryId(), current, queued + 1);
        return AdmissionDecision.queue(
            "concurrent query limit reached (" + current + "/"
                + quota.maxConcurrentQueries() + "), queued");
    }

    /** Memory-aware check after successful CAS: validate against quota then admit. */
    private AdmissionDecision checkMemoryAndAdmit(AdmissionRequest request, ResourceQuota quota,
                                                  AtomicInteger counter, String tenantId, int current) {
        if (request.estimatedMemoryBytes().isPresent()) {
            long estimated = request.estimatedMemoryBytes().getAsLong();
            if (estimated > quota.maxMemoryBytes()) {
                counter.decrementAndGet();
                LOG.info("event=admission_rejected tenantId={} queryId={} "
                                + "estimatedMemory={} maxMemory={}",
                        tenantId, request.queryId(), estimated, quota.maxMemoryBytes());
                return AdmissionDecision.reject(
                    "estimated memory " + estimated + " exceeds tenant limit "
                        + quota.maxMemoryBytes());
            }
            // P1-5 phase 2: reject if this query's estimate on top of the tenant's
            // current cluster-wide memory (heartbeat-fed) would exceed the budget.
            GlobalTenantUsageView view = this.globalUsageView;
            if (view != null) {
                long clusterUsed = view.aggregatedMemoryBytes(tenantId);
                if (clusterUsed + estimated > quota.maxMemoryBytes()) {
                    counter.decrementAndGet();
                    LOG.info("event=admission_rejected tenantId={} queryId={} "
                                    + "clusterMemory={} estimatedMemory={} maxMemory={}",
                            tenantId, request.queryId(), clusterUsed, estimated, quota.maxMemoryBytes());
                    return AdmissionDecision.reject(
                            "cluster memory " + clusterUsed + " + estimate " + estimated
                                    + " exceeds tenant limit " + quota.maxMemoryBytes());
                }
            }
        }
        LOG.debug("event=admission_admitted tenantId={} queryId={} concurrent={}",
                tenantId, request.queryId(), current + 1);
        return AdmissionDecision.admit();
    }

    /**
     * A-ADMISSION-01: admission gated by an ATOMIC cluster-wide reservation. The reservation is the
     * authoritative concurrency check; on memory rejection the reserved slot is released so it is not
     * leaked. The local {@code counter} is still tracked for release bookkeeping and diagnostics.
     */
    private AdmissionDecision admitViaClusterSlots(AdmissionRequest request, ResourceQuota quota,
            AtomicInteger counter, AtomicInteger queueCounter, String tenantId) {
        if (!clusterSlots.tryReserve(tenantId, quota.maxConcurrentQueries())) {
            return queueOrReject(request, quota, queueCounter, tenantId, clusterSlots.active(tenantId));
        }
        AdmissionDecision memoryReject = checkMemoryReject(request, quota, tenantId);
        if (memoryReject != null) {
            clusterSlots.release(tenantId);
            return memoryReject;
        }
        counter.incrementAndGet();
        LOG.debug("event=admission_admitted tenantId={} queryId={} clusterActive={}",
                tenantId, request.queryId(), clusterSlots.active(tenantId));
        return AdmissionDecision.admit();
    }

    /** A-ADMISSION-01: memory-only gate; returns a reject decision if over the memory quota, else null. */
    private AdmissionDecision checkMemoryReject(AdmissionRequest request, ResourceQuota quota, String tenantId) {
        if (request.estimatedMemoryBytes().isEmpty()) {
            return null;
        }
        long estimated = request.estimatedMemoryBytes().getAsLong();
        if (estimated > quota.maxMemoryBytes()) {
            return AdmissionDecision.reject(
                    "estimated memory " + estimated + " exceeds tenant limit " + quota.maxMemoryBytes());
        }
        GlobalTenantUsageView view = this.globalUsageView;
        if (view != null) {
            long clusterUsed = view.aggregatedMemoryBytes(tenantId);
            if (clusterUsed + estimated > quota.maxMemoryBytes()) {
                return AdmissionDecision.reject(
                        "cluster memory " + clusterUsed + " + estimate " + estimated
                                + " exceeds tenant limit " + quota.maxMemoryBytes());
            }
        }
        return null;
    }

    @Override
    public void release(String queryId, String tenantId) {
        // Determine whether this query was queued or admitted, and decrement the correct counter
        boolean wasQueued = queuedQueries.remove(queryId) != null;
        if (wasQueued) {
            AtomicInteger queueCounter = queuedCounts.get(tenantId);
            if (queueCounter != null) {
                int prev = queueCounter.decrementAndGet();
                if (prev < 0) {
                    queueCounter.set(0);
                }
            }
        } else {
            // A-ADMISSION-01: release the atomic cluster-wide slot (if using the distributed gate).
            if (clusterSlots != null) {
                clusterSlots.release(tenantId);
            }
            AtomicInteger counter = admittedCounts.get(tenantId);
            if (counter != null) {
                int prev = counter.decrementAndGet();
                if (prev < 0) {
                    counter.set(0);
                }
            }
        }
        LOG.debug("event=admission_released tenantId={} queryId={} wasQueued={}", tenantId, queryId, wasQueued);
    }

    @Override
    public int admittedCount(String tenantId) {
        // A-ADMISSION-01: the distributed slot store is the authoritative cluster-wide count.
        if (clusterSlots != null) {
            return clusterSlots.active(tenantId);
        }
        AtomicInteger counter = admittedCounts.get(tenantId);
        int local = counter != null ? counter.get() : 0;
        // SPI contract: admittedCount is a cluster-wide count, not coordinator-local.
        // Combine this coordinator's local in-flight admissions with the
        // heartbeat-fed cross-node active count (whichever is larger).
        GlobalTenantUsageView view = this.globalUsageView;
        if (view != null) {
            return Math.max(local, view.aggregatedActiveQueries(tenantId));
        }
        return local;
    }

    private void incrementAdmitted(String tenantId) {
        admittedCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).incrementAndGet();
    }
}
