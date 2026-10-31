package org.pangrova.coordination.inmemory;

import org.pangrova.coordination.api.DistributedLease;
import org.pangrova.coordination.api.LeaseEntry;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory distributed lease for single-node deployments and testing.
 *
 * <p>Concurrency strategy: all mutations use
 * {@link ConcurrentHashMap#compute} to guarantee atomicity without
 * external synchronization.</p>
 */
public final class InMemoryDistributedLease implements DistributedLease {

    private final ConcurrentHashMap<String, LeaseEntry> leases = new ConcurrentHashMap<>();
    private final Clock clock;

    public InMemoryDistributedLease() {
        this(Clock.systemUTC());
    }

    /**
     * Creates an in-memory lease store using an explicit time source.
     *
     * @param clock authoritative clock used for every lease decision
     */
    public InMemoryDistributedLease(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public Optional<LeaseEntry> tryAcquire(
            String leaseKey, String holderId, Duration duration) {
        requireRequest(leaseKey, holderId, duration);
        AtomicReference<LeaseEntry> acquired = new AtomicReference<>();
        leases.compute(leaseKey, (key, existing) -> {
            Instant now = clock.instant();
            Instant expiresAt = now.plusMillis(duration.toMillis());
            return resolveAcquire(existing, holderId, now, expiresAt, leaseKey, acquired);
        });
        return Optional.ofNullable(acquired.get());
    }

    private static LeaseEntry resolveAcquire(LeaseEntry existing, String holderId,
                                             Instant now, Instant expiresAt, String leaseKey,
                                             AtomicReference<LeaseEntry> acquired) {
        if (existing == null) {
            LeaseEntry granted = new LeaseEntry(leaseKey, holderId, 1, now, expiresAt);
            acquired.set(granted);
            return granted;
        }
        if (existing.isExpired(now)) {
            LeaseEntry granted = new LeaseEntry(leaseKey, holderId,
                    Math.incrementExact(existing.epoch()), now, expiresAt);
            acquired.set(granted);
            return granted;
        }
        if (existing.holderId().equals(holderId)) {
            acquired.set(existing);
        }
        return existing;
    }

    @Override
    public boolean tryRenew(
            String leaseKey, String holderId, long epoch, Duration duration) {
        requireRequest(leaseKey, holderId, duration);
        if (epoch <= 0) {
            throw new IllegalArgumentException("epoch must be positive");
        }
        boolean[] renewed = {false};
        Instant now = clock.instant();
        leases.computeIfPresent(leaseKey, (key, existing) -> {
            if (!existing.holderId().equals(holderId) || existing.epoch() != epoch) {
                renewed[0] = false;
                return existing;
            }
            if (existing.isExpired(now)) {
                renewed[0] = false;
                return existing;
            }
            renewed[0] = true;
            return new LeaseEntry(leaseKey, holderId, existing.epoch(),
                    existing.acquiredAt(), now.plusMillis(duration.toMillis()));
        });
        return renewed[0];
    }

    @Override
    public boolean release(String leaseKey, String holderId, long epoch) {
        if (leaseKey == null || holderId == null) {
            throw new NullPointerException("leaseKey and holderId must not be null");
        }
        if (epoch <= 0) {
            throw new IllegalArgumentException("epoch must be positive");
        }
        boolean[] released = {false};
        Instant now = clock.instant();
        leases.computeIfPresent(leaseKey, (key, existing) -> {
            if (existing.holderId().equals(holderId)
                    && existing.epoch() == epoch && !existing.isExpired(now)) {
                released[0] = true;
                return new LeaseEntry(key, holderId, epoch,
                        existing.acquiredAt(), now);
            }
            released[0] = false;
            return existing;
        });
        return released[0];
    }

    @Override
    public Optional<LeaseEntry> currentHolder(String leaseKey) {
        LeaseEntry entry = leases.get(leaseKey);
        if (entry != null && !entry.isExpired(clock.instant())) {
            return Optional.of(entry);
        }
        return Optional.empty();
    }

    /** Returns the number of retained lease high-water marks (for testing). */
    public int leaseCount() {
        return leases.size();
    }

    /** Returns the number of leases that have not expired at observation time. */
    public int activeLeaseCount() {
        Instant now = clock.instant();
        return (int) leases.values().stream()
                .filter(entry -> !entry.isExpired(now))
                .count();
    }

    /** Returns the lease entry for the given key (for testing). */
    public LeaseEntry getLease(String leaseKey) {
        return leases.get(leaseKey);
    }

    private static void requireRequest(
            String leaseKey, String holderId, Duration duration) {
        if (leaseKey == null || holderId == null || duration == null) {
            throw new NullPointerException(
                    "leaseKey, holderId and duration must not be null");
        }
        if (leaseKey.isBlank() || holderId.isBlank() || duration.toMillis() <= 0) {
            throw new IllegalArgumentException(
                    "leaseKey and holderId must be non-blank and duration must be positive");
        }
    }
}
