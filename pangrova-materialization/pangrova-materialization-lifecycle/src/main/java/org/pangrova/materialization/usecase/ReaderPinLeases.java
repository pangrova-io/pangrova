package org.pangrova.materialization.usecase;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.domain.QueryId;
import org.pangrova.materialization.api.domain.ReaderPin;
import org.pangrova.materialization.api.port.Clock;
import org.pangrova.persistence.materialization.api.lifecycle.MaterializationRepository;

/**
 * Leases reader visibility of published generations for the lifetime of a query.
 *
 * <p>A query leases every generation it plans to scan before execution starts
 * and releases all of them when it reaches terminal cleanup. Long-running
 * queries renew their lease periodically; a coordinator that dies stops
 * renewing, so its pins expire and garbage collection can proceed.
 */
public final class ReaderPinLeases {

    private static final Duration DEFAULT_LEASE = Duration.ofMinutes(5);

    private final MaterializationRepository repository;
    private final Clock clock;
    private final Duration lease;
    private final Set<QueryId> leased = ConcurrentHashMap.newKeySet();

    public ReaderPinLeases(MaterializationRepository repository, Clock clock) {
        this(repository, clock, DEFAULT_LEASE);
    }

    /**
     * Creates a lease registry with an explicit lease duration.
     *
     * @param repository durable reader pin store
     * @param clock time source
     * @param lease pin lease duration
     */
    public ReaderPinLeases(MaterializationRepository repository, Clock clock, Duration lease) {
        this.repository = Objects.requireNonNull(repository, "repository");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (lease == null || lease.isZero() || lease.isNegative()) {
            throw new IllegalArgumentException("reader pin lease must be positive");
        }
        this.lease = lease;
    }

    /**
     * Leases a generation for a query.
     *
     * @param generation generation the query will scan
     * @param query owning query
     * @param owner query owner identity
     */
    public void acquire(GenerationId generation, QueryId query, String owner) {
        Instant now = clock.now();
        repository.pinReader(new ReaderPin(generation, query, owner,
                now.plus(lease), now));
        leased.add(query);
    }

    /**
     * Releases every generation leased by a query.
     *
     * @param query owning query
     */
    public void release(QueryId query) {
        if (!leased.contains(query)) {
            return;
        }
        repository.releaseReaderPins(query);
        leased.remove(query);
    }

    /**
     * Renews every generation leased by a still-running query.
     *
     * @param queries owning queries
     */
    public void renew(Collection<QueryId> queries) {
        Instant now = clock.now();
        RuntimeException failure = null;
        for (QueryId query : queries) {
            if (!leased.contains(query)) {
                continue;
            }
            try {
                repository.renewReaderPins(query, now.plus(lease), now);
            } catch (RuntimeException renewalFailure) {
                if (failure == null) {
                    failure = renewalFailure;
                } else {
                    failure.addSuppressed(renewalFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Deletes pins whose lease already expired.
     *
     * @return number of deleted pins
     */
    public int purgeExpired() {
        return repository.purgeExpiredReaderPins(clock.now());
    }

    /** Renewal period that keeps an active query well inside its lease. */
    public Duration renewInterval() {
        return lease.dividedBy(3);
    }

    /** Lease granted to each query. */
    public Duration lease() {
        return lease;
    }
}
