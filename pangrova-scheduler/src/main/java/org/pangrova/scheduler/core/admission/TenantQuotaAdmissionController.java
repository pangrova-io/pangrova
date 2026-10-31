package org.pangrova.scheduler.core.admission;

import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.tenant.TenantStatus;
import org.pangrova.security.api.tenant.TenantStore;
import org.pangrova.scheduler.api.resource.AdmissionDecision;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.resource.ClusterAdmissionController;
import org.pangrova.scheduler.api.quota.GlobalTenantUsageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Checks tenant status, concurrent-query limits and memory estimates before dispatch.
 *
 * <p>Limits come from {@link TenantStore}. Local counters use CAS; an explicitly
 * supplied {@link ClusterConcurrencySlots} is the shared concurrency authority.
 * A heartbeat-fed usage view is observational, not an atomic memory reservation.</p>
 *
 * <p>Concurrency model: per-tenant {@link AtomicInteger} counters in a
 * {@link ConcurrentHashMap}, incremented atomically on admit and decremented
 * on release. Memory-aware admission is supported when the
 * {@link AdmissionRequest} carries a CBO estimate.</p>
 *
 * <p>This admission check does not install Worker runtime quotas or enforce
 * CPU, scan or daily usage limits during execution.</p>
 */
public final class TenantQuotaAdmissionController implements ClusterAdmissionController {

    private static final Logger LOG = LoggerFactory.getLogger(TenantQuotaAdmissionController.class);

    private final TenantStore tenantStore;
    /** Optional cluster-wide usage view fed by worker heartbeats; null = local-only. */
    private final GlobalTenantUsageView globalUsageView;

    /**
     * Explicit shared concurrency gate; null uses only the local reservation counter.
     */
    private final ClusterConcurrencySlots clusterSlots;

    /** Per-tenant admitted (in-flight) query counts. */
    private final ConcurrentHashMap<String, AtomicInteger> admittedCounts = new ConcurrentHashMap<>();

    /** Per-tenant queued query counts. */
    private final ConcurrentHashMap<String, AtomicInteger> queuedCounts = new ConcurrentHashMap<>();

    private final QueryAdmissionOwnership ownership = new QueryAdmissionOwnership();

    /**
     * Creates a process-local admission controller.
     *
     * @param tenantStore authority for tenant definitions and limits
     */
    public TenantQuotaAdmissionController(TenantStore tenantStore) {
        this(tenantStore, null, null);
    }

    /**
     * Creates a controller that additionally enforces cluster-wide limits using a
     * heartbeat-fed {@link GlobalTenantUsageView}. When {@code globalUsageView} is
     * null the controller enforces only this coordinator's local counters
     * (single-node semantics).
     *
     * @param tenantStore authority for tenant definitions and limits
     * @param globalUsageView observational usage view, or null for local counters only
     */
    public TenantQuotaAdmissionController(TenantStore tenantStore, GlobalTenantUsageView globalUsageView) {
        this(tenantStore, globalUsageView, null);
    }

    /**
     * Creates a controller with an explicitly supplied shared concurrency gate.
     * Without a shared gate, concurrency reservation is local to this controller.
     *
     * @param tenantStore authority for tenant definitions and limits
     * @param globalUsageView observational usage view, or null
     * @param clusterSlots shared concurrency authority, or null for local reservation
     */
    public TenantQuotaAdmissionController(TenantStore tenantStore, GlobalTenantUsageView globalUsageView,
                                          ClusterConcurrencySlots clusterSlots) {
        this.tenantStore = Objects.requireNonNull(tenantStore, "tenantStore must not be null");
        this.globalUsageView = globalUsageView;
        this.clusterSlots = clusterSlots;
    }

    @Override
    public AdmissionDecision admit(AdmissionRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        return ownership.admit(request, reservation -> admitNew(request, reservation));
    }

    private AdmissionDecision admitNew(AdmissionRequest request, QueryAdmissionOwnership.Reservation reservation) {
        String tenantId = request.tenantId();

        Tenant tenant = tenantStore.getTenant(tenantId).orElse(null);
        AdmissionDecision tenantRejection = rejectInactiveTenant(tenantId, tenant);
        if (tenantRejection != null) {
            return tenantRejection;
        }

        ResourceQuota quota = tenant.quota();
        if (quota == null) {
            quota = new ResourceQuota(Long.MAX_VALUE, Integer.MAX_VALUE,
                    0, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        }

        AtomicInteger counter = admittedCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0));
        AtomicInteger queueCounter = queuedCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0));

        // A shared slot store supplies the concurrency reservation; a lagged usage view cannot.
        if (clusterSlots != null) {
            return admitViaClusterSlots(request, quota, counter, queueCounter, tenantId, reservation);
        }
        return admitViaLocalCounters(request, quota, counter, queueCounter, tenantId, reservation);
    }

    /**
     * Admission gated by this coordinator's local counter, optionally pre-checked against the
     * heartbeat-fed cluster view. Extracted from {@link #admit} to keep both methods within the
     * project cyclomatic-complexity budget, and to mirror {@link #admitViaClusterSlots}.
     */
    private AdmissionDecision admitViaLocalCounters(AdmissionRequest request, ResourceQuota quota,
            AtomicInteger counter, AtomicInteger queueCounter, String tenantId,
            QueryAdmissionOwnership.Reservation reservation) {
        // Observed usage may reject known saturation, but cannot reserve across coordinators.
        GlobalTenantUsageView view = this.globalUsageView;
        if (view != null) {
            int clusterActive = view.aggregatedActiveQueries(tenantId);
            if (clusterActive >= quota.maxConcurrentQueries()) {
                return queueOrReject(request, quota, queueCounter, tenantId, clusterActive, reservation);
            }
        }

        // CAS loop: atomically check against concurrent query limit
        while (true) {
            int current = counter.get();
            if (current >= quota.maxConcurrentQueries()) {
                return queueOrReject(request, quota, queueCounter, tenantId, current, reservation);
            }
            if (counter.compareAndSet(current, current + 1)) {
                reservation.retain(counter::decrementAndGet);
                return checkMemoryAndAdmit(request, quota, tenantId, current, reservation);
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
                                            AtomicInteger queueCounter, String tenantId, int current,
                                            QueryAdmissionOwnership.Reservation reservation) {
        int queued;
        do {
            queued = queueCounter.get();
            if (queued < quota.maxQueuedQueries()) {
                continue;
            }
            LOG.info("event=admission_rejected tenantId={} queryId={} "
                            + "concurrent={} max={} queued={} maxQueued={}",
                    tenantId, request.queryId(), current,
                    quota.maxConcurrentQueries(), queued, quota.maxQueuedQueries());
            return AdmissionDecision.reject(
                    "concurrent query limit reached (" + current + "/"
                        + quota.maxConcurrentQueries() + ") and queue full ("
                        + queued + "/" + quota.maxQueuedQueries() + ")");
        } while (!queueCounter.compareAndSet(queued, queued + 1));
        reservation.retain(queueCounter::decrementAndGet);
        LOG.debug("event=admission_queued tenantId={} queryId={} concurrent={} queued={}",
                tenantId, request.queryId(), current, queued + 1);
        return AdmissionDecision.queue(
            "concurrent query limit reached (" + current + "/"
                + quota.maxConcurrentQueries() + "), queued");
    }

    /** Memory-aware check after successful CAS: validate against quota then admit. */
    private AdmissionDecision checkMemoryAndAdmit(AdmissionRequest request, ResourceQuota quota,
                                                  String tenantId, int current,
                                                  QueryAdmissionOwnership.Reservation reservation) {
        AdmissionDecision rejected = checkMemoryReject(request, quota, tenantId);
        if (rejected != null) {
            reservation.returnCapacity();
            LOG.info("event=admission_rejected tenantId={} queryId={} reason={}",
                    tenantId, request.queryId(), rejected.reason());
            return rejected;
        }
        LOG.debug("event=admission_admitted tenantId={} queryId={} concurrent={}",
                tenantId, request.queryId(), current + 1);
        return AdmissionDecision.admit();
    }

    /**
     * Reserves through the shared gate and balances memory rejection with its actual release.
     * The local counter supplies release bookkeeping and diagnostics.
     */
    private AdmissionDecision admitViaClusterSlots(AdmissionRequest request, ResourceQuota quota,
            AtomicInteger counter, AtomicInteger queueCounter, String tenantId,
            QueryAdmissionOwnership.Reservation reservation) {
        reservation.retain(() -> clusterSlots.release(tenantId, request.queryId()));
        if (!clusterSlots.tryReserve(tenantId, request.queryId(), quota.maxConcurrentQueries())) {
            reservation.returnCapacity();
            return queueOrReject(request, quota, queueCounter, tenantId, clusterSlots.active(tenantId), reservation);
        }
        AdmissionDecision memoryReject = checkMemoryReject(request, quota, tenantId);
        if (memoryReject != null) {
            reservation.returnCapacity();
            return memoryReject;
        }
        counter.incrementAndGet();
        reservation.retain(() -> {
            clusterSlots.release(tenantId, request.queryId());
            counter.decrementAndGet();
        });
        LOG.debug("event=admission_admitted tenantId={} queryId={} clusterActive={}",
                tenantId, request.queryId(), clusterSlots.active(tenantId));
        return AdmissionDecision.admit();
    }

    /** Returns a memory rejection, or null when the available estimate fits. */
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
            if (clusterUsed < 0) {
                return AdmissionDecision.reject("invalid negative cluster memory usage: " + clusterUsed);
            }
            if (clusterUsed > quota.maxMemoryBytes() || estimated > quota.maxMemoryBytes() - clusterUsed) {
                return AdmissionDecision.reject(
                        "cluster memory " + clusterUsed + " + estimate " + estimated
                                + " exceeds tenant limit " + quota.maxMemoryBytes());
            }
        }
        return null;
    }

    @Override
    public void release(String queryId, String tenantId) {
        if (!ownership.release(queryId, tenantId) && clusterSlots != null) {
            // A replacement process may return a persisted exact reservation after physical proof.
            clusterSlots.release(tenantId, queryId);
        }
    }

    @Override
    public int admittedCount(String tenantId) {
        // A shared slot store, when supplied, is the reservation-count authority.
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

}
