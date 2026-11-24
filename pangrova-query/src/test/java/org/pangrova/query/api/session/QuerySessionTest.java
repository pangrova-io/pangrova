package org.pangrova.query.api.session;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pangrova.common.json.CanonicalJsonMapper;
import org.pangrova.connector.api.metadata.table.SnapshotRange;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.connector.api.query.ConnectorQueryOperation;
import org.pangrova.observability.api.tracing.ExecutionTraceContext;
import org.pangrova.observability.api.tracing.Scope;
import org.pangrova.security.api.identity.UserIdentity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QuerySessionTest {
    private final ExecutionTraceContext trace = new UnactivatedTrace();

    @Test
    void copiesPreserveBorrowedAuthorityAndEveryTrustedSelectorWithoutActivatingOrClosing() {
        var authority = new UnusedAuthority("query");
        var actor = new UserIdentity("actor", Set.of(), Map.of(), Instant.EPOCH);
        var original = new QuerySession("query", Map.of("catalog", "catalog"), actor, "unresolved",
                Optional.of(authority), trace, Map.of("catalog.schema.exact", 17L),
                Map.of("catalog.schema.delta", new SnapshotRange(29, 23)), true);
        var copy = original.withTenantId("resolved").withSessionProperties(Map.of("schema", "schema"))
                .withUserIdentity(actor).withQueryAuthority(authority).withTraceContext(trace);
        assertThat(copy.queryAuthority()).containsSame(authority);
        assertThat(copy.traceContext()).isSameAs(trace);
        assertThat(copy.tenantId()).isEqualTo("resolved");
        assertThat(copy.queryId()).isEqualTo(original.queryId());
        assertThat(copy.sourceSnapshots()).isEqualTo(original.sourceSnapshots());
        assertThat(copy.sourceIncrementalRanges()).isEqualTo(original.sourceIncrementalRanges());
        assertThat(copy.internalMaterializationExecution()).isTrue();
        assertThat(copy.userIdentity()).isSameAs(actor);
        assertThat(original.tenantId()).isEqualTo("unresolved");
        assertThat(original.sessionProperties()).containsOnlyKeys("catalog");
    }

    @Test
    void journalAdmissionPreservesTheExactIdentity() throws Exception {
        var actor = new UserIdentity("alice\"\\team", Set.of(), Map.of(), Instant.EPOCH);
        var session = new QuerySession("query\"1", Map.of("schema", "private"), actor, null,
                Optional.empty(), trace);

        String payload = JournalSessionPayload.encode(session);
        var json = CanonicalJsonMapper.readTree(payload);
        assertThat(json.size()).isEqualTo(3);
        assertThat(json.path("queryId").textValue()).isEqualTo(session.queryId());
        assertThat(json.path("tenantId").textValue()).isEmpty();
        assertThat(JournalSessionPayload.admittedPrincipal(session.queryId(), payload))
                .contains(actor.principal());
        assertThat(JournalSessionPayload.admittedPrincipal("another-query", payload)).isEmpty();
    }

    @Test
    void journalRecoveryDoesNotCoerceMalformedOrNonTextIdentity() {
        assertThat(JournalSessionPayload.admittedPrincipal("query", "{broken")).isEmpty();
        assertThat(JournalSessionPayload.admittedPrincipal("query",
                "{\"queryId\":\"query\",\"principal\":42}")).isEmpty();
        assertThat(JournalSessionPayload.admittedPrincipal("query",
                "{\"queryId\":\"query\",\"principal\":\" \"}")).isEmpty();
    }

    @Test
    void authorityCannotBeReplacedOrReboundToAnotherQueryIdentity() {
        var authority = new UnusedAuthority("query");
        var session = new QuerySession("query", Map.of(), null, null, Optional.empty(), trace);
        var admitted = session.withQueryAuthority(authority);
        assertThatThrownBy(() -> admitted.withQueryId("other")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> admitted.withQueryAuthority(new UnusedAuthority("query")))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> session.withQueryAuthority(new UnusedAuthority("other")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(session.withQueryId("other").traceContext()).isSameAs(trace);
    }

    @Test
    void mutableInputsAreFrozenAndExactAndIncrementalSelectionsCannotOverlap() {
        var snapshots = new HashMap<>(Map.of("table", 42L));
        var props = new HashMap<>(Map.of("source_snapshots", "untrusted-client-text"));
        var session = new QuerySession("query", props, null, null, Optional.empty(), trace)
                .withSourceSnapshots(snapshots);
        snapshots.put("table", 99L);
        props.clear();
        assertThat(session.sourceSnapshots()).containsEntry("table", 42L);
        assertThat(session.sessionProperties()).containsKey("source_snapshots");
        assertThat(session.internalMaterializationExecution()).isFalse();
        assertThatThrownBy(() -> session.sourceSnapshots().put("other", 1L))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> session.withSourceIncrementalRanges(Map.of("table", new SnapshotRange(1, 2))))
                .isInstanceOf(IllegalArgumentException.class);
        var fromClientProperties = new QuerySession("client", session.sessionProperties(), null, null,
                Optional.empty(), trace);
        assertThat(fromClientProperties.sourceSnapshots()).isEmpty();
        assertThat(fromClientProperties.sourceIncrementalRanges()).isEmpty();
        assertThat(fromClientProperties.internalMaterializationExecution()).isFalse();
    }

    @Test
    void missingCorrelationCannotDiscoverAnImplicitProvider() {
        assertThatThrownBy(() -> new QuerySession("query", Map.of(), null, null, Optional.empty(), null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("traceContext");
        assertThatThrownBy(() -> new QuerySession("query", Map.of(), null, null, null, trace))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("queryAuthority");
    }

    private record UnusedAuthority(String queryId) implements ConnectorQueryAuthority {
        public boolean isClosed() { throw new AssertionError("Value copies must not inspect lifetime state"); }
        public void requireDrained() { throw new AssertionError("Value copies must not drain the owner"); }
        public ConnectorQueryOperation enterOperation() { throw new AssertionError("Value copies must not enter work"); }
    }

    private static final class UnactivatedTrace implements ExecutionTraceContext {
        public String traceId() { return "00000000000000000000000000000001"; }
        public String spanId() { return "0000000000000001"; }
        public int traceFlags() { return 0; }
        public String traceState() { return ""; }
        public Scope makeCurrent() { throw new AssertionError("Value copies must not activate a scope"); }
    }
}
