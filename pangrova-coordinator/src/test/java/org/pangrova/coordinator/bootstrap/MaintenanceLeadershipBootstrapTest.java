package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.index.lifecycle.MaintenanceLeaderElection;
import org.pangrova.spi.coordination.MaintenanceLeaderAware;

import java.time.Duration;
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

    private static MaintenanceLeadershipBootstrap bootstrap(
            InMemoryDistributedLease lease, String owner,
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
}
