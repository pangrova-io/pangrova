package org.pangrova.scheduler.admission;

import org.pangrova.persistence.coordination.api.admission.ClusterConcurrencySlots;

import org.pangrova.scheduler.api.resource.AdmissionDecision;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.resource.AdmissionRequest.TenantLimits;
import org.pangrova.scheduler.api.admission.ClusterAdmissionController;
import org.pangrova.scheduler.api.quota.GlobalTenantUsageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reserves capacity for an authorized tenant before dispatch.
 *
 * <p>Limits come from the authenticated submission. Local counters use CAS; an explicitly
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

    /** Optional cluster-wide usage view fed by worker heartbeats; null = local-only. */
    private final GlobalTenantUsageView globalUsageView;

    /**
     * Explicit shared concurrency gate; null uses only the local reservation counter.
     */
    private final ClusterConcurrencySlots clusterSlots;

    /** Per-tenant admitted (in-flight) query counts. */
    private final ConcurrentHashMap<String, AtomicInteger> admittedCounts = new ConcurrentHashMap<>();

    private final QueryAdmissionOwnership ownership = new QueryAdmissionOwnership();

    /**
     * Creates a controller with an explicitly supplied shared concurrency gate.
     * Without a shared gate, concurrency reservation is local to this controller.
     *
     * @param globalUsageView observational usage view, or null
     * @param clusterSlots shared concurrency authority, or null for local reservation
     */
    public TenantQuotaAdmissionController(GlobalTenantUsageView globalUsageView, ClusterConcurrencySlots clusterSlots) {
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
        TenantLimits quota = request.tenantLimits();
        AdmissionDecision memoryReject = checkMemoryReject(request, quota, tenantId);
        if (memoryReject != null) return memoryReject;

        AtomicInteger counter = admittedCounts.computeIfAbsent(tenantId, key -> new AtomicInteger());
        // The shared gate owns concurrency when present.
        if (clusterSlots != null) {
            return admitViaClusterSlots(request, quota, counter, reservation);
        }
        return admitViaLocalCounters(request, quota, counter, reservation);
    }

    private AdmissionDecision admitViaLocalCounters(AdmissionRequest request, TenantLimits quota,
            AtomicInteger counter, QueryAdmissionOwnership.Reservation reservation) {
        String tenantId = request.tenantId();
        GlobalTenantUsageView view = globalUsageView;
        if (view != null && view.aggregatedActiveQueries(tenantId) >= quota.maxConcurrentQueries()) {
            return rejectCapacity(request, quota);
        }

        while (true) {
            int current = counter.get();
            if (current >= quota.maxConcurrentQueries()) return rejectCapacity(request, quota);
            if (counter.compareAndSet(current, current + 1)) {
                reservation.retain(counter::decrementAndGet);
                LOG.debug("event=admission_admitted tenantId={} queryId={} localActive={}",
                        tenantId, request.queryId(), current + 1);
                return AdmissionDecision.admit();
            }
        }
    }

    private AdmissionDecision admitViaClusterSlots(AdmissionRequest request, TenantLimits quota,
            AtomicInteger counter, QueryAdmissionOwnership.Reservation reservation) {
        String tenantId = request.tenantId();
        reservation.retain(() -> clusterSlots.release(tenantId, request.queryId()));
        if (!clusterSlots.tryReserve(tenantId, request.queryId(), quota.maxConcurrentQueries())) {
            reservation.clearCapacity();
            return rejectCapacity(request, quota);
        }
        int localActive = counter.incrementAndGet();
        reservation.retain(() -> {
            clusterSlots.release(tenantId, request.queryId());
            counter.decrementAndGet();
        });
        LOG.debug("event=admission_admitted tenantId={} queryId={} localActive={}",
                tenantId, request.queryId(), localActive);
        return AdmissionDecision.admit();
    }

    private static AdmissionDecision rejectCapacity(AdmissionRequest request, TenantLimits quota) {
        LOG.info("event=admission_rejected tenantId={} queryId={} maxConcurrent={}",
                request.tenantId(), request.queryId(), quota.maxConcurrentQueries());
        return AdmissionDecision.reject("concurrent query limit reached (capacity "
                + quota.maxConcurrentQueries() + ")");
    }

    /** Returns a memory rejection, or null when the available estimate fits. */
    private AdmissionDecision checkMemoryReject(AdmissionRequest request, TenantLimits quota, String tenantId) {
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
