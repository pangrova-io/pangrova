package org.pangrova.persistence.coordination.memory.membership;
import org.pangrova.coordination.api.membership.WorkerRegistrationIdentity;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot;
import org.pangrova.persistence.coordination.api.membership.WorkerRegistry;
import org.pangrova.coordination.api.membership.WorkerSnapshot;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Internal
public final class InMemoryWorkerRegistry implements WorkerRegistry {

    private final ConcurrentHashMap<String, WorkerSnapshot> workers = new ConcurrentHashMap<>();

    @Override
    public void register(WorkerSnapshot worker) {
        Objects.requireNonNull(worker, "worker must not be null");
        workers.put(worker.workerId(), worker);
    }

    @Override
    public boolean refreshRegistration(WorkerSnapshot worker) {
        Objects.requireNonNull(worker, "worker must not be null");
        var accepted = new java.util.concurrent.atomic.AtomicBoolean();
        workers.compute(worker.workerId(), (id, existing) -> {
            if (existing != null && !existing.registrationIdentity().equals(worker.registrationIdentity())) {
                return existing;
            }
            accepted.set(true);
            return worker;
        });
        return accepted.get();
    }

    @Override
    public Optional<WorkerSnapshot> heartbeat(
            WorkerRegistrationIdentity expected, HeartbeatSnapshot snapshot) {
        Objects.requireNonNull(expected, "expected must not be null");
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        // F-CHAIN-CACHE-02: preserve cacheSummary on heartbeat. Previously
        // dropped silently here, breaking the cache-locality chain.
        var updated = new java.util.concurrent.atomic.AtomicReference<WorkerSnapshot>();
        workers.computeIfPresent(expected.workerId(), (id, existing) -> {
            if (!existing.registrationIdentity().equals(expected)) return existing;
            WorkerSnapshot next = new WorkerSnapshot(
                existing.workerId(),
                existing.host(),
                existing.httpPort(),
                existing.transportPort(),
                existing.protocolVersion(),
                existing.rackId(),
                existing.dataLocality(),
                snapshot.memoryPressure(),
                snapshot.cpuUtilization(),
                snapshot.runningTaskCount(),
                snapshot.availableMemoryBytes(),
                System.currentTimeMillis(),
                existing.registeredAtMs(),
                existing.resultServerPort(),
                snapshot.cacheSummary() != null ? snapshot.cacheSummary() : existing.cacheSummary(),
                existing.capabilityInventory(),
                existing.copyTaskTlsPort(), existing.admissionDomainId(), existing.incarnationId()
            );
            updated.set(next);
            return next;
        });
        return Optional.ofNullable(updated.get());
    }

    @Override
    public boolean unregister(org.pangrova.coordination.api.membership.WorkerRegistrationIdentity expected) {
        Objects.requireNonNull(expected, "expected must not be null");
        var removed = new java.util.concurrent.atomic.AtomicBoolean();
        workers.computeIfPresent(expected.workerId(), (id, current) -> {
            if (!current.registrationIdentity().equals(expected)) return current;
            removed.set(true);
            return null;
        });
        return removed.get();
    }

    @Override
    public List<WorkerSnapshot> getActiveWorkers(Duration staleThreshold) {
        Objects.requireNonNull(staleThreshold, "staleThreshold must not be null");
        long now = System.currentTimeMillis();
        long thresholdMs = staleThreshold.toMillis();
        List<WorkerSnapshot> active = workers.values().stream()
                .filter(w -> now - w.lastHeartbeatMs() < thresholdMs)
                .toList();
        return List.copyOf(active);
    }

    @Override
    public Optional<WorkerSnapshot> getWorker(String workerId) {
        Objects.requireNonNull(workerId, "workerId must not be null");
        return Optional.ofNullable(workers.get(workerId));
    }

    @Override
    public int expireStale(Duration threshold) {
        Objects.requireNonNull(threshold, "threshold must not be null");
        long now = System.currentTimeMillis();
        long thresholdMs = threshold.toMillis();
        AtomicInteger count = new AtomicInteger(0);
        workers.forEach((id, worker) -> {
            if (now - worker.lastHeartbeatMs() >= thresholdMs) {
                expireIfStale(id, now, thresholdMs, count);
            }
        });
        return count.get();
    }

    private void expireIfStale(String id, long now, long thresholdMs, AtomicInteger count) {
        workers.computeIfPresent(id, (k, current) -> {
            if (now - current.lastHeartbeatMs() >= thresholdMs) {
                count.incrementAndGet();
                return null;
            }
            return current;
        });
    }

    public int workerCount() {
        return workers.size();
    }
}
