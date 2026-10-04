package org.pangrova.security.tenant.quota;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.resource.admission.AdmissionDecision;
import org.pangrova.spi.resource.admission.AdmissionRequest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/** Owns the admission and physical-release boundary for each live query in this process. */
@ThreadSafe
final class QueryAdmissionOwnership {

    private final ConcurrentHashMap<String, Entry> queries = new ConcurrentHashMap<>();

    AdmissionDecision admit(AdmissionRequest request, AdmissionAction admission) {
        var candidate = new Entry(request);
        var existing = queries.putIfAbsent(request.queryId(), candidate);
        if (existing != null) {
            if (!existing.request.equals(request)) {
                return AdmissionDecision.reject("query admission identity does not match existing owner");
            }
            return existing.decision.join();
        }
        try {
            var decision = admission.run(candidate.reservation);
            candidate.decision.complete(decision);
            if (decision.verdict() == org.pangrova.spi.resource.admission.AdmissionVerdict.REJECT) {
                queries.remove(request.queryId(), candidate);
            }
            return decision;
        } catch (RuntimeException | Error failure) {
            candidate.decision.completeExceptionally(failure);
            // Admission may have reached the shared store before the failure. Keep its owner.
            throw failure;
        }
    }

    boolean release(String queryId, String tenantId) {
        var entry = queries.get(queryId);
        if (entry == null || !entry.request.tenantId().equals(tenantId)) {
            return false;
        }
        // Wait for admission to exit, including an ambiguous shared-store outcome.
        entry.decision.handle((decision, failure) -> null).join();
        var candidate = new CompletableFuture<Void>();
        CompletableFuture<Void> existing;
        synchronized (entry) {
            existing = entry.release;
            if (existing == null) {
                entry.release = candidate;
            }
        }
        if (existing != null) {
            existing.join();
            return true;
        }
        try {
            entry.reservation.release.run();
            queries.remove(queryId, entry);
            candidate.complete(null);
            return true;
        } catch (RuntimeException | Error failure) {
            synchronized (entry) {
                entry.release = null;
            }
            candidate.completeExceptionally(failure);
            throw failure;
        }
    }

    private static final class Entry {
        private final AdmissionRequest request;
        private final CompletableFuture<AdmissionDecision> decision = new CompletableFuture<>();
        private final Reservation reservation = new Reservation();
        private CompletableFuture<Void> release;

        private Entry(AdmissionRequest request) {
            this.request = request;
        }
    }

    @FunctionalInterface
    interface AdmissionAction {
        AdmissionDecision run(Reservation reservation);
    }

    static final class Reservation {
        private Runnable release = () -> {};

        void retain(Runnable release) {
            this.release = release;
        }

        void returnCapacity() {
            release.run();
            release = () -> {};
        }
    }
}
