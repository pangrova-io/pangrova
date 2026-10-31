package org.pangrova.testing.integration.e2e.coordination;

import org.pangrova.coordinator.coordination.LeaseBackedLeaderElection;
import org.pangrova.coordination.api.LeaseEntry;

import java.util.Objects;
import java.util.Optional;

/**
 * E2E driver that preserves the backend-issued fencing epoch when exercising
 * the leader-election lifecycle. It intentionally does not expose an API for
 * caller-supplied epochs.
 */
final class FencedLeaderElectionTestDriver {

    private final LeaseBackedLeaderElection delegate;

    FencedLeaderElectionTestDriver(LeaseBackedLeaderElection delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    }

    boolean tryAcquire(String candidateId, long leaseDurationMs) {
        return delegate.tryAcquire(candidateId, leaseDurationMs).isPresent();
    }

    boolean tryRenew(String leaderId, long leaseDurationMs) {
        return currentEntryFor(leaderId)
                .map(entry -> delegate.tryRenew(
                        leaderId, entry.epoch(), leaseDurationMs))
                .orElse(false);
    }

    boolean release(String leaderId) {
        return currentEntryFor(leaderId)
                .map(entry -> delegate.release(leaderId, entry.epoch()))
                .orElse(false);
    }

    Optional<String> currentLeader() {
        return delegate.currentLeader();
    }

    private Optional<LeaseEntry> currentEntryFor(String holderId) {
        return delegate.currentLeaderEntry()
                .filter(entry -> entry.holderId().equals(holderId));
    }
}
