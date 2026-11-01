package org.pangrova.coordination.inmemory;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.spi.coordination.HeartbeatSnapshot;
import org.pangrova.persistence.api.coordination.WorkerRegistry;
import org.pangrova.spi.coordination.WorkerSnapshot;

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
    public void heartbeat(String workerId, HeartbeatSnapshot snapshot) {
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        // F-CHAIN-CACHE-02: preserve cacheSummary on heartbeat. Previously
        // dropped silently here, breaking the cache-locality chain.
        workers.computeIfPresent(workerId, (id, existing) -> new WorkerSnapshot(
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
        ));
    }

    @Override
    public void unregister(String workerId) {
        Objects.requireNonNull(workerId, "workerId must not be null");
        workers.remove(workerId);
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
