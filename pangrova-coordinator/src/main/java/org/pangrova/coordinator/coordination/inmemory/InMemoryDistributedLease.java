package org.pangrova.coordinator.coordination.inmemory;

import org.pangrova.spi.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory distributed lease for single-node deployments and testing.
 *
 * <p>Concurrency strategy: all mutations use
 * {@link ConcurrentHashMap#compute} to guarantee atomicity without
 * external synchronization.</p>
 */
public final class InMemoryDistributedLease implements DistributedLease {

    private final ConcurrentHashMap<String, LeaseEntry> leases = new ConcurrentHashMap<>();

    @Override
    public boolean tryAcquire(String leaseKey, String holderId, long epoch, Duration duration) {
        boolean[] acquired = {false};
        leases.compute(leaseKey, (key, existing) -> {
            Instant now = Instant.now();
            Instant expiresAt = now.plus(duration);
            return resolveAcquire(existing, holderId, epoch, now, expiresAt, leaseKey, acquired);
        });
        return acquired[0];
    }

    private static LeaseEntry resolveAcquire(LeaseEntry existing, String holderId, long epoch,
                                             Instant now, Instant expiresAt, String leaseKey,
                                             boolean[] acquired) {
        if (existing == null || existing.isExpired() || epoch > existing.epoch()) {
            acquired[0] = true;
            return new LeaseEntry(leaseKey, holderId, epoch, now, expiresAt);
        }
        if (existing.holderId().equals(holderId) && epoch >= existing.epoch()) {
            acquired[0] = true;
            return new LeaseEntry(leaseKey, holderId, epoch, now, expiresAt);
        }
        acquired[0] = false;
        return existing;
    }

    @Override
    public boolean tryRenew(String leaseKey, String holderId, Duration duration) {
        boolean[] renewed = {false};
        leases.computeIfPresent(leaseKey, (key, existing) -> {
            if (!existing.holderId().equals(holderId)) {
                renewed[0] = false;
                return existing;
            }
            if (existing.isExpired()) {
                renewed[0] = false;
                return existing;
            }
            Instant now = Instant.now();
            renewed[0] = true;
            return new LeaseEntry(leaseKey, holderId, existing.epoch() + 1,
                    now, now.plus(duration));
        });
        return renewed[0];
    }

    @Override
    public boolean release(String leaseKey, String holderId) {
        boolean[] released = {false};
        leases.computeIfPresent(leaseKey, (key, existing) -> {
            if (existing.holderId().equals(holderId)) {
                released[0] = true;
                return null;
            }
            released[0] = false;
            return existing;
        });
        return released[0];
    }

    @Override
    public Optional<LeaseEntry> currentHolder(String leaseKey) {
        LeaseEntry entry = leases.get(leaseKey);
        if (entry != null && !entry.isExpired()) {
            return Optional.of(entry);
        }
        return Optional.empty();
    }

    @Override
    public int expireStale() {
        AtomicInteger count = new AtomicInteger(0);
        leases.forEach((key, entry) -> {
            if (entry.isExpired()) {
                expireIfStale(key, count);
            }
        });
        return count.get();
    }

    private void expireIfStale(String key, AtomicInteger count) {
        leases.computeIfPresent(key, (k, current) -> {
            if (current.isExpired()) {
                count.incrementAndGet();
                return null;
            }
            return current;
        });
    }

    /** Returns the number of active lease entries (for testing). */
    public int leaseCount() {
        return leases.size();
    }

    /** Returns the lease entry for the given key (for testing). */
    public LeaseEntry getLease(String leaseKey) {
        return leases.get(leaseKey);
    }
}
