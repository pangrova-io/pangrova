package org.pangrova.worker.bootstrap.ownership;

import org.junit.jupiter.api.Test;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleChannelAdmission;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleClientPool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual partial bootstrap channel owners, not a simulated Worker startup. */
class WorkerTaskOwnershipTest {
    @Test void failedPartialCleanupRetainsCreditsBlocksReplacementAndTriesEveryActualPool() {
        var refs = new WorkerComponentRefs();
        var ownership = new WorkerTaskOwnership(new RemoteShuffleChannelAdmission.Limits(2, 1));
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
            assertThat(ownership.channelAdmission().snapshot().ownedAndCreatingChannels()).isEqualTo(1);
            assertThatThrownBy(() -> second.borrow(endpoint(9401))).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> refs.installTaskOwnership(new WorkerTaskOwnership(
                    new RemoteShuffleChannelAdmission.Limits(2, 1))))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Previous task bootstrap owners");
        } finally {
            lease.close();
            ownership.close();
        }
        assertThat(actual.isTerminated()).isTrue();
        assertThat(ownership.isDrained()).isTrue();
        ownership.channelAdmission().requireDrained();
        var replacement = new WorkerTaskOwnership(new RemoteShuffleChannelAdmission.Limits(1, 1));
        refs.installTaskOwnership(replacement);
        assertThat(refs.taskOwnership()).isSameAs(replacement);
        replacement.close();
    }

    @Test void sealedOwnershipRetainsLateActualOwnerForCleanupRetry() {
        var ownership = new WorkerTaskOwnership(new RemoteShuffleChannelAdmission.Limits(1, 1));
        ownership.close();
        var late = pool(ownership);
        assertThatThrownBy(() -> ownership.retain(late)).isInstanceOf(IllegalStateException.class);
        assertThat(ownership.isDrained()).isFalse();
        ownership.close();
        assertThat(ownership.isDrained()).isTrue();
        assertThatThrownBy(() -> late.borrow(endpoint(9400))).isInstanceOf(IllegalStateException.class);
    }

    @Test void actualAssemblyOwnerCreatedAfterManagerPublicationIsStillDrainedAndRetryable() {
        var ownership = new WorkerTaskOwnership(new RemoteShuffleChannelAdmission.Limits(1, 1));
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
            assertThat(ownership.channelAdmission().snapshot().ownedAndCreatingChannels()).isEqualTo(1);
            assertThat(actualChannel.isTerminated()).isFalse();
        } finally {
            lease.close();
            ownership.close();
        }
        assertThat(actualChannel.isTerminated()).isTrue();
        assertThat(ownership.isDrained()).isTrue();
        ownership.channelAdmission().requireDrained();
    }

    private static RemoteShuffleClientPool pool(WorkerTaskOwnership ownership) {
        return new RemoteShuffleClientPool(5_000, 5_000, RemoteShuffleClientPool.PoolOptions.defaults(),
                ownership.channelAdmission());
    }

    private static ShuffleServerEndpoint endpoint(int port) {
        return new ShuffleServerEndpoint("localhost", port, 0, 1, "");
    }
}
