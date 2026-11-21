package org.pangrova.query.lifecycle;

import org.pangrova.query.api.enums.QueryState;


import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;

/** Attribution belongs to a query state machine, not a shared emitter-side identity cache. */
class QueryStateMachineAuditTest {
    @Test
    void directStateMachineHasExplicitAnonymousAttribution() {
        assertThat(new QueryStateMachine().getAuditPrincipal()).isEqualTo("anonymous");
        assertThat(new QueryStateMachine(false, "anonymous")
                .getAuditPrincipal()).isEqualTo("anonymous");
    }

    @Test
    void capturesSubjectBeforeOwnershipAndKeepsQueriesIsolated() {
        var alice = new QueryStateMachine(true, "alice");
        var bob = new QueryStateMachine(true, "bob");
        alice.setTenantId("tenant-a");
        bob.setTenantId("tenant-b");
        alice.resolveOwnership(true);
        bob.resolveOwnership(false);
        assertThat(alice.getAuditPrincipal()).isEqualTo("alice");
        assertThat(bob.getAuditPrincipal()).isEqualTo("bob");
        assertThat(alice.getTenantId()).isEqualTo("tenant-a");
        assertThat(bob.getTenantId()).isEqualTo("tenant-b");
    }

    @Test
    void rejectedOwnershipStillPublishesFailureWithoutClaimingPhysicalCleanup() {
        var state = new QueryStateMachine(true);
        state.resolveOwnership(false);
        var error = new org.pangrova.query.api.error.QueryFailure(
                "QUERY_ADMISSION_FAILED", "QueryRejectedException", "Cluster admission denied");
        assertThat(state.tryTransitionToFailed(error)).isTrue();
        assertThat(state.getState()).isEqualTo(QueryState.FAILED);
        assertThat(state.getError()).contains(error);
        assertThat(state.physicalCleanupApplied()).isFalse();
        assertThat(state.tryTransition(QueryState.FINISHED)).isFalse();
    }

    @Test
    void logicalTerminalDoesNotCloseAnInstalledReservationOwner() throws Exception {
        var pipe = java.nio.channels.Pipe.open();
        try (var sink = pipe.sink(); var source = pipe.source()) {
            var state = new QueryStateMachine();
            state.retainAdmissionRelease(() -> {
                try { sink.close(); }
                catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
            });
            assertThat(state.tryTransitionToFailed(new org.pangrova.query.api.error.QueryFailure(
                    "QUERY_FAILED", "Failure", "Actual cleanup is pending"))).isTrue();
            assertThat(sink.isOpen()).isTrue();
            assertThat(source.isOpen()).isTrue();
            state.releaseAdmission();
            assertThat(sink.isOpen()).isFalse();
            state.releaseAdmission();
            assertThat(state.physicalCleanupApplied()).isFalse();
        }
    }

    @Test
    void suppliedUnknownPrincipalIsNotRewrittenAsAnAnonymousIdentity() {
        assertThat(new QueryStateMachine(true, (String) null).getAuditPrincipal()).isNull();
    }
}
