package org.pangrova.coordinator.query;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.query.api.enums.QueryState;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.connector.api.metadata.ConnectorMetadataScope;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real query-state and issued-operation ownership, without a fabricated lifecycle token. */
class ConnectorQueryAuthorityTest {
    @Test
    void metadataCapabilityRequiresTheActualStateBoundOwner() {
        var tracker = new CoordinatorTaskTracker();
        var issued = tracker.openQueryIssuance("scope-capability");
        assertThatThrownBy(() -> ConnectorMetadataScope.require(issued))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("public query state authority");
        var state = new QueryStateMachine();
        var bound = state.bindConnectorAuthority(issued);
        assertThat(ConnectorMetadataScope.require(bound)).isSameAs(bound);
        assertThat(state.connectorAuthority()).isSameAs(bound);
        state.tryTransition(QueryState.CANCELLED);
        tracker.sealQueryIssuance(bound.queryId());
        bound.cleanupMetadata().requireSuccess();
        tracker.retireQueryIssuance(bound.queryId());
    }

    @Test
    void sealDoesNotProveReaderOperationExitAndCleanupRemainsRetryable() {
        var tracker = new CoordinatorTaskTracker();
        var state = new QueryStateMachine();
        var authority = state.bindConnectorAuthority(tracker.openQueryIssuance("owned-reader"));
        var reader = authority.enterOperation();
        state.tryTransition(QueryState.CANCELLED);
        tracker.sealQueryIssuance(authority.queryId());
        assertThat(authority.isClosed()).isTrue();
        assertThatThrownBy(authority::requireDrained).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(authority::enterOperation).isInstanceOf(IllegalStateException.class);
        reader.close();
        authority.requireDrained();
        authority.requireDrained();
        tracker.retireQueryIssuance(authority.queryId());
        authority.requireDrained();
        assertThatThrownBy(authority::enterOperation).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void recorderFailureRollsBackPendingAdmissionWithoutInventingDrainEvidence() throws Exception {
        var tracker = new CoordinatorTaskTracker();
        var state = new QueryStateMachine();
        var authority = state.bindConnectorAuthority(tracker.openQueryIssuance("record-failure"));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        state.setTransitionRecorder((previous, next) -> {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Recorder release timed out");
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(failure);
            }
            throw new IllegalStateException("Actual terminal recorder failed");
        });
        var executor = Executors.newSingleThreadExecutor();
        try {
            var transition = executor.submit(() -> state.tryTransition(QueryState.CANCELLED));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(authority.isClosed()).isFalse();
            assertThatThrownBy(authority::enterOperation).hasMessageContaining("admission is closed");
            assertThatThrownBy(authority::cleanupMetadata).hasMessageContaining("closed query admission");
            release.countDown();
            assertThatThrownBy(() -> transition.get(5, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Actual terminal recorder failed");
            assertThat(state.getState()).isEqualTo(QueryState.QUEUED);
            assertThat(authority.isClosed()).isFalse();
            try (var operation = authority.enterOperation()) {
                assertThatThrownBy(authority::requireDrained).isInstanceOf(IllegalStateException.class);
            }
            state.setTransitionRecorder((previous, next) -> { });
            state.tryTransition(QueryState.CANCELLED);
            tracker.sealQueryIssuance(authority.queryId());
            authority.requireDrained();
            tracker.retireQueryIssuance(authority.queryId());
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
