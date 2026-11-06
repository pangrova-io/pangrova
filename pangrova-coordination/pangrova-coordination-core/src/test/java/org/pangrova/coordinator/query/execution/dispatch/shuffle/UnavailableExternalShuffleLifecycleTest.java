package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchContext;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.shuffle.api.ShuffleRegistration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnavailableExternalShuffleLifecycleTest {
    @Test
    void dagDefaultRejectsExternalRegistrationAndCommitInsteadOfReturningLocalSuccess() {
        var context = new DAGDispatchContext((request, worker) -> {
            throw new AssertionError("No task should be dispatched by this construction test");
        }, null, null, null, 0, null, null, null, null, null, null,
                new SplitAssignmentManager(), null, null);
        var lifecycle = context.shuffleLifecycleManager();
        assertThat(lifecycle).isSameAs(UnavailableExternalShuffleLifecycle.INSTANCE);
        assertThatThrownBy(() -> lifecycle.registerShuffle(
                new ShuffleRegistration("q", 0, 1, 0, false, 1, 1, new byte[32])))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("not configured");
        assertThatThrownBy(() -> lifecycle.commitShuffle("q", 0))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("not configured");
        lifecycle.cleanupShuffle("q");
        lifecycle.cleanupShuffle("q");
    }

    @Test
    void invalidCleanupCannotSilentlyBecomeAnAcceptedQuery() {
        assertThatThrownBy(() -> UnavailableExternalShuffleLifecycle.INSTANCE.cleanupShuffle(" "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> UnavailableExternalShuffleLifecycle.INSTANCE.commitShuffle("q", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
