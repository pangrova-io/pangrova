package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.adapter.indexing.lifecycle.MaintenanceLeaderElection;
import org.pangrova.coordination.api.MaintenanceLeaderAware;
import org.pangrova.coordination.api.DistributedLease;
import org.pangrova.coordination.api.LeaseEntry;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaintenanceLeadershipBootstrapTest {

    @Test
    void maintenanceLeadershipIsIndependentAndTransfersAfterRelease() {
        var lease = new InMemoryDistributedLease();
        var firstListener = new RecordingListener();
        var secondListener = new RecordingListener();
        var first = bootstrap(lease, "maintenance-a", firstListener);
        var second = bootstrap(lease, "maintenance-b", secondListener);
        try {
            first.poll();
            second.poll();

            assertThat(first.isLeader()).isTrue();
            assertThat(first.activeEpoch()).isPositive();
            assertThat(second.isLeader()).isFalse();
            assertThat(firstListener.elected).hasValue(1);
            assertThat(secondListener.elected).hasValue(0);

            first.close();
            second.poll();

            assertThat(firstListener.lost).hasValue(1);
            assertThat(second.isLeader()).isTrue();
            assertThat(secondListener.elected).hasValue(1);
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    void repeatedPollRenewsWithoutRepeatingLifecycleCallback() {
        var lease = new InMemoryDistributedLease();
        var listener = new RecordingListener();
        var bootstrap = bootstrap(lease, "maintenance-a", listener);
        try {
            bootstrap.poll();
            bootstrap.poll();
            bootstrap.poll();

            assertThat(listener.elected).hasValue(1);
            assertThat(listener.lost).hasValue(0);
            assertThat(bootstrap.activeEpoch()).isPositive();
        } finally {
            bootstrap.close();
        }
    }

    @Test
    void closeReleasesLeaseEvenWhenDemotionCallbackFails() {
        var lease = new InMemoryDistributedLease();
        MaintenanceLeaderAware failingListener = new MaintenanceLeaderAware() {
            @Override
            public void onLeaderElected() {
            }

            @Override
            public void onLeadershipLost() {
                throw new IllegalStateException("stop failed");
            }
        };
        var first = bootstrap(lease, "maintenance-a", failingListener);
        var secondListener = new RecordingListener();
        var second = bootstrap(lease, "maintenance-b", secondListener);
        try {
            first.poll();

            assertThatThrownBy(first::close)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("stop failed");

            // The failed local callback must never strand the distributed lease.
            second.poll();
            assertThat(second.isLeader()).isTrue();
            assertThat(secondListener.elected).hasValue(1);
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    void renewalFailureDemotesBeforeReturningAndSuccessfulRenewalReactivates() {
        var lease = new FailingRenewalLease();
        var listener = new RecordingListener();
        try (var bootstrap = bootstrap(lease, "maintenance-a", listener)) {
            bootstrap.poll();
            lease.failRenewal = true;
            assertThatThrownBy(bootstrap::poll).hasMessage("lease backend unavailable");
            assertThat(bootstrap.isLeader()).isFalse();
            assertThat(bootstrap.activeEpoch()).isZero();
            assertThat(listener.lost).hasValue(1);
            lease.failRenewal = false;
            bootstrap.poll();
            assertThat(bootstrap.isLeader()).isTrue();
            assertThat(listener.elected).hasValue(2);
        }
    }

    @Test
    void failedActivationStopsPartiallyStartedSubsystems() {
        var stopped = new AtomicInteger();
        var listener = new MaintenanceLeaderAware() {
            @Override public void onLeaderElected() {
                throw new IllegalStateException("activation failed after starting a subsystem");
            }
            @Override public void onLeadershipLost() { stopped.incrementAndGet(); }
        };
        try (var bootstrap = bootstrap(new InMemoryDistributedLease(), "maintenance-a", listener)) {
            assertThatThrownBy(bootstrap::poll).hasMessage("activation failed after starting a subsystem");
            assertThat(bootstrap.isLeader()).isFalse();
            assertThat(bootstrap.activeEpoch()).isZero();
            assertThat(stopped).hasValue(1);
        }
    }

    @Test
    void aNewBackendEpochDemotesAndReactivatesTheLocalSubsystems() {
        var lease = new InMemoryDistributedLease();
        var listener = new RecordingListener();
        try (var bootstrap = bootstrap(lease, "maintenance-a", listener)) {
            bootstrap.poll();
            long previous = bootstrap.activeEpoch();
            assertThat(lease.release("maintenance-control", "maintenance-a", previous)).isTrue();
            bootstrap.poll();
            assertThat(bootstrap.activeEpoch()).isGreaterThan(previous);
            assertThat(listener.lost).hasValue(1);
            assertThat(listener.elected).hasValue(2);
        }
    }

    private static MaintenanceLeadershipBootstrap bootstrap(
            DistributedLease lease, String owner,
            MaintenanceLeaderAware listener) {
        return new MaintenanceLeadershipBootstrap(
                new MaintenanceLeaderElection(lease, owner), listener,
                Duration.ofSeconds(1), Executors.newSingleThreadScheduledExecutor());
    }

    private static final class RecordingListener implements MaintenanceLeaderAware {
        private final AtomicInteger elected = new AtomicInteger();
        private final AtomicInteger lost = new AtomicInteger();

        @Override
        public void onLeaderElected() {
            elected.incrementAndGet();
        }

        @Override
        public void onLeadershipLost() {
            lost.incrementAndGet();
        }
    }

    private static final class FailingRenewalLease implements DistributedLease {
        private final InMemoryDistributedLease delegate = new InMemoryDistributedLease();
        private boolean failRenewal;
        @Override public Optional<LeaseEntry> tryAcquire(String key, String holder, Duration duration) {
            return delegate.tryAcquire(key, holder, duration);
        }
        @Override public boolean tryRenew(String key, String holder, long epoch, Duration duration) {
            if (failRenewal) throw new IllegalStateException("lease backend unavailable");
            return delegate.tryRenew(key, holder, epoch, duration);
        }
        @Override public boolean release(String key, String holder, long epoch) {
            return delegate.release(key, holder, epoch);
        }
        @Override public Optional<LeaseEntry> currentHolder(String key) { return delegate.currentHolder(key); }
    }
}
