package org.pangrova.coordinator.query;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.security.UserIdentity;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Attribution belongs to a query state machine, not a shared emitter-side identity cache. */
class QueryStateMachineAuditTest {
    @Test
    void directStateMachineHasExplicitAnonymousAttribution() {
        assertThat(new QueryStateMachine().getAuditPrincipal()).isEqualTo("anonymous");
        assertThat(new QueryStateMachine(false, new CoordinatorSessionContext("query", Map.of(), null, null))
                .getAuditPrincipal()).isEqualTo("anonymous");
    }

    @Test
    void capturesSubjectBeforeOwnershipAndKeepsQueriesIsolated() {
        var alice = new QueryStateMachine(true, session("alice-query", "alice"));
        var bob = new QueryStateMachine(true, session("bob-query", "bob"));
        alice.setTenantId("tenant-a");
        bob.setTenantId("tenant-b");
        alice.resolveOwnership(true);
        bob.resolveOwnership(false);
        assertThat(alice.getAuditPrincipal()).isEqualTo("alice");
        assertThat(bob.getAuditPrincipal()).isEqualTo("bob");
        assertThat(alice.getTenantId()).isEqualTo("tenant-a");
        assertThat(bob.getTenantId()).isEqualTo("tenant-b");
    }

    private static CoordinatorSessionContext session(String queryId, String principal) {
        return new CoordinatorSessionContext(queryId, Map.of(),
                new UserIdentity(principal, Set.of(), Map.of(), Instant.EPOCH), null);
    }
}
