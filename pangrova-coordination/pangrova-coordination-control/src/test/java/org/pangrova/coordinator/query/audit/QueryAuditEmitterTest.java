package org.pangrova.coordinator.query.audit;

import org.pangrova.query.audit.QueryAuditEmitter;
import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.query.api.enums.QueryState;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.query.memory.audit.InMemoryQueryAuditStore;
import org.pangrova.query.audit.QueryAuditService;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.api.enums.AuditEventType;

import java.time.Instant;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Checks asynchronous terminal event attribution without retaining per-query session maps. */
class QueryAuditEmitterTest {
    @Test
    void cancellationPublishesItsOwnSecurityEventAndUnsuccessfulQueryHistory() {
        var events = new java.util.ArrayList<AuditEvent>();
        var service = new QueryAuditService(new InMemoryQueryAuditStore(4));
        var emitter = new QueryAuditEmitter(events::add, service);
        var audit = QueryAuditRecord.wallTimeOnly(
                "cancelled-query", "alice", "SELECT 42", "tenant-a", 41, Instant.now(), false);
        emitter.emitQueryTerminal(QueryState.CANCELLED, audit, null);
        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.eventType()).isEqualTo(AuditEventType.QUERY_CANCELLED);
            assertThat(event.principal()).isEqualTo("alice");
            assertThat(event.success()).isFalse();
            assertThat(event.errorMessage()).isNull();
        });
        var record = service.findByQueryId("cancelled-query", "alice").orElseThrow();
        assertThat(record).isSameAs(audit);
        assertThat(record.success()).isFalse();
        assertThat(record.tenantId()).isEqualTo("tenant-a");
        assertThat(record.wallTimeMs()).isEqualTo(41);
        assertThat(record.sql()).isEqualTo("SELECT 42");
    }

    @Test
    void completedRecordKeepsTheSubmittingIdentityWithoutAnEventBackend() {
        var service = new QueryAuditService(new InMemoryQueryAuditStore(4));
        var emitter = new QueryAuditEmitter(null, service);
        emitter.emitQueryTerminal(QueryState.FINISHED, record("completed-query", "alice", "tenant-a", 17, true), null);

        var record = service.findByQueryId("completed-query", "alice").orElseThrow();
        assertThat(record.userId()).isEqualTo("alice");
        assertThat(record.tenantId()).isEqualTo("tenant-a");
        assertThat(record.wallTimeMs()).isEqualTo(17);
        assertThat(record.success()).isTrue();
    }

    @Test
    void failedRecordKeepsItsOwnIdentityAfterAnotherTenantCompletes() {
        var service = new QueryAuditService(new InMemoryQueryAuditStore(4));
        var emitter = new QueryAuditEmitter(null, service);
        emitter.emitQueryTerminal(QueryState.FINISHED, record("alice-query", "alice", "tenant-a", 17, true), null);
        emitter.emitQueryTerminal(QueryState.FAILED, record("bob-query", "bob", "tenant-b", 23, false), "denied");

        var record = service.findByQueryId("bob-query", "bob").orElseThrow();
        assertThat(record.userId()).isEqualTo("bob");
        assertThat(record.tenantId()).isEqualTo("tenant-b");
        assertThat(record.wallTimeMs()).isEqualTo(23);
        assertThat(record.success()).isFalse();
        assertThat(service.findByQueryId("alice-query", "alice").orElseThrow().tenantId()).isEqualTo("tenant-a");
    }

    @Test
    void anonymousRejectionDoesNotInventAPersistedTenant() {
        var service = new QueryAuditService(new InMemoryQueryAuditStore(4));
        var emitter = new QueryAuditEmitter(null, service);
        emitter.emitQueryTerminal(QueryState.FAILED, record("anonymous-query", "anonymous", null, 7, false), "rejected");

        var record = service.findByQueryId("anonymous-query", "anonymous").orElseThrow();
        assertThat(record.userId()).isEqualTo("anonymous");
        assertThat(record.tenantId()).isNull();
        assertThat(record.success()).isFalse();
    }

    @Test
    void completedAndFailedEventsKeepTheirOwnSubjectAndTenant() {
        var events = new CopyOnWriteArrayList<AuditEvent>();
        var backend = new AuditEventEmitter(events::add);
        try {
            var emitter = new QueryAuditEmitter(backend::emit, null);
            emitter.emitQueryTerminal(QueryState.FINISHED, record("alice-query", "alice", "tenant-a", 17, true), null);
            emitter.emitQueryTerminal(QueryState.FAILED, record("bob-query", "bob", "tenant-b", 23, false), "denied");
            org.awaitility.Awaitility.await().atMost(3, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(events).hasSize(2));
            assertThat(events.get(0).eventType()).isEqualTo(AuditEventType.QUERY_COMPLETED);
            assertThat(events.get(0).principal()).isEqualTo("alice");
            assertThat(events.get(0).properties()).containsEntry("tenant_id", "tenant-a");
            assertThat(events.get(0).success()).isTrue();
            assertThat(events.get(1).eventType()).isEqualTo(AuditEventType.QUERY_FAILED);
            assertThat(events.get(1).principal()).isEqualTo("bob");
            assertThat(events.get(1).properties()).containsEntry("tenant_id", "tenant-b");
            assertThat(events.get(1).success()).isFalse();
            assertThat(events.get(1).errorMessage()).isEqualTo("denied");
        } finally {
            backend.close();
        }
    }

    @Test
    void anonymousSubmissionAndRejectionStayExplicitWithoutInventingATenant() {
        var events = new CopyOnWriteArrayList<AuditEvent>();
        var backend = new AuditEventEmitter(events::add);
        try {
            var emitter = new QueryAuditEmitter(backend::emit, null);
            emitter.emitQuerySubmitted("anonymous-query", "SELECT 1", null);
            emitter.emitQueryTerminal(QueryState.FAILED, record("anonymous-query", "anonymous", null, 7, false),
                    "anonymous rejected");
            org.awaitility.Awaitility.await().atMost(3, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(events).hasSize(2));
            assertThat(events).allSatisfy(event -> {
                assertThat(event.principal()).isEqualTo("anonymous");
                assertThat(event.properties()).doesNotContainKey("tenant_id");
            });
        } finally {
            backend.close();
        }
    }

    @Test
    void attributionIsMandatoryEvenWithoutBackends() {
        var emitter = QueryAuditEmitter.noOp();
        assertThatThrownBy(() -> record("query", null, "tenant", 0, false))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("principal");
        assertThatThrownBy(() -> emitter.emitQueryTerminal(QueryState.FINISHED, null, null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("record");
        assertThatThrownBy(() -> emitter.emitQueryTerminal(QueryState.FAILED, null, "denied"))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("record");
    }

    private static QueryAuditRecord record(String queryId, String principal, String tenant, long millis,
            boolean success) {
        return QueryAuditRecord.wallTimeOnly(queryId, principal, null, tenant, millis, Instant.now(), success);
    }
}
