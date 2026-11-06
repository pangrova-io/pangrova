package org.pangrova.testing.integration.e2e.fixture;

import org.pangrova.persistence.coordination.postgresql.lease.JdbcDistributedLease;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.coordination.api.lease.LeaseEntry;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Isolates a test's real lease operations without resetting shared fencing epochs. */
public final class TestLeaseNamespace {

    private final String prefix = "e2e-" + UUID.randomUUID() + ":";

    public DistributedLease jdbc(DataSource dataSource) {
        return wrap(new JdbcDistributedLease(dataSource));
    }

    public DistributedLease wrap(DistributedLease delegate) {
        Objects.requireNonNull(delegate, "delegate must not be null");
        return new DistributedLease() {
            @Override
            public Optional<LeaseEntry> tryAcquire(
                    String leaseKey, String holderId, Duration duration) {
                return delegate.tryAcquire(physicalKey(leaseKey), holderId, duration)
                        .map(entry -> logicalEntry(leaseKey, entry));
            }

            @Override
            public boolean tryRenew(
                    String leaseKey, String holderId, long epoch, Duration duration) {
                return delegate.tryRenew(physicalKey(leaseKey), holderId, epoch, duration);
            }

            @Override
            public boolean release(String leaseKey, String holderId, long epoch) {
                return delegate.release(physicalKey(leaseKey), holderId, epoch);
            }

            @Override
            public Optional<LeaseEntry> currentHolder(String leaseKey) {
                return delegate.currentHolder(physicalKey(leaseKey))
                        .map(entry -> logicalEntry(leaseKey, entry));
            }
        };
    }

    private String physicalKey(String leaseKey) {
        return prefix + Objects.requireNonNull(leaseKey, "leaseKey must not be null");
    }

    private static LeaseEntry logicalEntry(String leaseKey, LeaseEntry entry) {
        return new LeaseEntry(leaseKey, entry.holderId(), entry.epoch(),
                entry.acquiredAt(), entry.expiresAt());
    }
}
