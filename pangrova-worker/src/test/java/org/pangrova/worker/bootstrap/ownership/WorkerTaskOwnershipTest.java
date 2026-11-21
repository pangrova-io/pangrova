package org.pangrova.worker.bootstrap.ownership;

import org.junit.jupiter.api.Test;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleClientPool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual partial bootstrap channel owners, not a simulated Worker startup. */
class WorkerTaskOwnershipTest {
    @Test void failedPartialCleanupRetainsCreditsBlocksReplacementAndTriesEveryActualPool() {
        var refs = new WorkerComponentRefs();
        var ownership = ownership(2, 1);
        refs.installTaskOwnership(ownership);
        var first = pool(ownership);
        var second = pool(ownership);
        ownership.retain(first);
        ownership.retain(second);
        var lease = first.borrow(endpoint(9400));
        var actual = lease.client();
        try {
            assertThatThrownBy(ownership::close).isInstanceOf(IllegalStateException.class);
            assertThat(ownership.isDrained()).isFalse();
            assertThat(actual.isTerminated()).isFalse();
            assertThat(first.workerOwnedChannelCount()).isEqualTo(1);
            assertThatThrownBy(() -> second.borrow(endpoint(9401))).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> refs.installTaskOwnership(ownership(2, 1)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Previous task bootstrap owners");
        } finally {
            lease.close();
            ownership.close();
        }
        assertThat(actual.isTerminated()).isTrue();
        assertThat(ownership.isDrained()).isTrue();
        ownership.shuffle().requireDrained();
        var replacement = ownership(1, 1);
        refs.installTaskOwnership(replacement);
        assertThat(refs.taskOwnership()).isSameAs(replacement);
        replacement.close();
    }

    @Test void partialConstructionTransfersTheSoleCloseOrRetainsTheUnclaimedResource() {
        var ownership = new WorkerTaskOwnership();
        var closed = new java.util.ArrayList<String>();
        AutoCloseable first = () -> closed.add("first");
        AutoCloseable second = () -> closed.add("second");
        ownership.retain(first, resource -> (AutoCloseable) () -> {
            closed.add("owner");
            resource.close();
        });
        assertThatThrownBy(() -> ownership.retain(second, resource -> {
            throw new IllegalStateException("construct");
        })).hasMessage("construct");
        ownership.close();
        ownership.close();
        assertThat(closed).containsExactly("second", "owner", "first");
    }

    @Test void sealedOwnershipRetainsLateActualOwnerForCleanupRetry() {
        var ownership = ownership(1, 1);
        var late = pool(ownership);
        ownership.close();
        assertThatThrownBy(() -> ownership.retain(late)).isInstanceOf(IllegalStateException.class);
        assertThat(ownership.isDrained()).isFalse();
        ownership.close();
        assertThat(ownership.isDrained()).isTrue();
        assertThatThrownBy(() -> late.borrow(endpoint(9400))).isInstanceOf(IllegalStateException.class);
    }

    @Test void actualAssemblyOwnerCreatedAfterManagerPublicationIsStillDrainedAndRetryable() {
        var ownership = ownership(1, 1);
        // The actual HTTP-only manager owns no executor; this tests ownership,
        // not task execution or a synthesized terminal report.
        ownership.publish(org.pangrova.worker.task.TaskManager.forHttpWiringOnly());
        var actualPool = pool(ownership);
        ownership.retain(actualPool);
        var lease = actualPool.borrow(endpoint(9400));
        var actualChannel = lease.client();
        try {
            assertThatThrownBy(ownership::close).isInstanceOf(IllegalStateException.class);
            assertThat(ownership.isDrained()).isFalse();
            assertThat(actualPool.workerOwnedChannelCount()).isEqualTo(1);
            assertThat(actualChannel.isTerminated()).isFalse();
        } finally {
            lease.close();
            ownership.close();
        }
        assertThat(actualChannel.isTerminated()).isTrue();
        assertThat(ownership.isDrained()).isTrue();
        ownership.shuffle().requireDrained();
    }

    private static WorkerTaskOwnership ownership(int total, int perEndpoint) {
        var factory = java.util.ServiceLoader.load(org.pangrova.shuffle.nativewire.NativeShuffleWorkerFactory.class)
                .stream().map(java.util.ServiceLoader.Provider::get).filter(f -> f.providerId().equals("pangrova"))
                .findFirst().orElseThrow();
        return new WorkerTaskOwnership(factory.open(new org.pangrova.shuffle.nativewire.NativeShuffleWorkerFactory.Configuration(
                new org.pangrova.shuffle.api.worker.ShuffleChannelLimits(total, perEndpoint), null, java.util.Map.of())));
    }

    private static RemoteShuffleClientPool pool(WorkerTaskOwnership ownership) {
        return (RemoteShuffleClientPool) ownership.shuffle().readers();
    }

    private static ShuffleServerEndpoint endpoint(int port) {
        return new ShuffleServerEndpoint("localhost", port, 0, 1, "");
    }
}
