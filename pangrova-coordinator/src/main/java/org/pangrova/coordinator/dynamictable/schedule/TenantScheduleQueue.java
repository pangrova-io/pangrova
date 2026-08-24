package org.pangrova.coordinator.dynamictable.schedule;

import org.pangrova.common.concurrent.ThreadSafe;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-tenant concurrency and quota tracker for Dynamic Table refreshes.
 *
 * <p>Concurrency strategy: all counters use {@link AtomicInteger} with
 * CAS operations — no external synchronization required.</p>
 */
@ThreadSafe
public final class TenantScheduleQueue {

    private final String tenantId;
    private final int maxConcurrent;
    private final int maxDynamicTables;
    private final AtomicInteger registeredCount = new AtomicInteger(0);
    private final AtomicInteger activeCount = new AtomicInteger(0);

    public TenantScheduleQueue(String tenantId, int maxConcurrent, int maxDynamicTables) {
        this.tenantId = Objects.requireNonNull(tenantId, "tenantId must not be null");
        this.maxConcurrent = maxConcurrent;
        this.maxDynamicTables = maxDynamicTables;
    }

    public boolean tryStartRefresh() {
        while (true) {
            int current = activeCount.get();
            if (current >= maxConcurrent) {
                return false;
            }
            if (activeCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    public void completeRefresh() {
        activeCount.updateAndGet(v -> Math.max(0, v - 1));
    }

    public boolean canRegister() {
        return registeredCount.get() < maxDynamicTables;
    }

    public void register() {
        registeredCount.incrementAndGet();
    }

    public void unregister() {
        registeredCount.updateAndGet(v -> Math.max(0, v - 1));
    }

    public int queueDepth() {
        return registeredCount.get();
    }

    public int activeCount() {
        return activeCount.get();
    }

    public String tenantId() {
        return tenantId;
    }
}
