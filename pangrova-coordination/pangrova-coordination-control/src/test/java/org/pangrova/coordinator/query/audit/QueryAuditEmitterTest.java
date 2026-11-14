package org.pangrova.coordinator.query.audit;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.query.memory.audit.InMemoryQueryAuditStore;
import org.pangrova.query.audit.QueryAuditService;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.api.enums.AuditEventType;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Checks asynchronous terminal event attribution without retaining per-query session maps. */
class QueryAuditEmitterTest {
    @Test
    void completedRecordKeepsTheSubmittingIdentityWithoutAnEventBackend() {
        var service = new QueryAuditService(new InMemoryQueryAuditStore(4));
        var emitter = new QueryAuditEmitter(null, service);
        emitter.emitQueryCompleted("completed-query", 17,
                new QueryAuditEmitter.AuditSubject("alice", "tenant-a"));

        var record = service.findByQueryId("completed-query").orElseThrow();
        assertThat(record.userId()).isEqualTo("alice");
        assertThat(record.tenantId()).isEqualTo("tenant-a");
        assertThat(record.wallTimeMs()).isEqualTo(17);
        assertThat(record.success()).isTrue();
    }

    @Test
    void failedRecordKeepsItsOwnIdentityAfterAnotherTenantCompletes() {
        var service = new QueryAuditService(new InMemoryQueryAuditStore(4));
        var emitter = new QueryAuditEmitter(null, service);
        emitter.emitQueryCompleted("alice-query", 17,
                new QueryAuditEmitter.AuditSubject("alice", "tenant-a"));
        emitter.emitQueryFailed("bob-query", "denied", 23,
                new QueryAuditEmitter.AuditSubject("bob", "tenant-b"));

        var record = service.findByQueryId("bob-query").orElseThrow();
        assertThat(record.userId()).isEqualTo("bob");
        assertThat(record.tenantId()).isEqualTo("tenant-b");
        assertThat(record.wallTimeMs()).isEqualTo(23);
        assertThat(record.success()).isFalse();
        assertThat(service.findByQueryId("alice-query").orElseThrow().tenantId()).isEqualTo("tenant-a");
    }

    @Test
    void anonymousRejectionDoesNotInventAPersistedTenant() {
        var service = new QueryAuditService(new InMemoryQueryAuditStore(4));
        var emitter = new QueryAuditEmitter(null, service);
        emitter.emitQueryFailed("anonymous-query", "rejected", 7,
                new QueryAuditEmitter.AuditSubject("anonymous", null));

        var record = service.findByQueryId("anonymous-query").orElseThrow();
        assertThat(record.userId()).isEqualTo("anonymous");
        assertThat(record.tenantId()).isNull();
        assertThat(record.success()).isFalse();
    }

    @Test
    void completedAndFailedEventsKeepTheirOwnSubjectAndTenant() {
        var events = new CopyOnWriteArrayList<AuditEvent>();
        var backend = new AuditEventEmitter(events::add);
        try {
            var emitter = new QueryAuditEmitter(backend, null);
            emitter.emitQueryCompleted("alice-query", 17,
                    new QueryAuditEmitter.AuditSubject("alice", "tenant-a"));
            emitter.emitQueryFailed("bob-query", "denied", 23,
                    new QueryAuditEmitter.AuditSubject("bob", "tenant-b"));
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
            var emitter = new QueryAuditEmitter(backend, null);
            emitter.emitQuerySubmitted("anonymous-query", "SELECT 1", null);
            emitter.emitQueryFailed("anonymous-query", "anonymous rejected", 7,
                    new QueryAuditEmitter.AuditSubject("anonymous", null));
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
        assertThatThrownBy(() -> new QueryAuditEmitter.AuditSubject(null, "tenant"))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("principal");
        assertThatThrownBy(() -> emitter.emitQueryCompleted("query", 0, null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("subject");
        assertThatThrownBy(() -> emitter.emitQueryFailed("query", "denied", 0, null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("subject");
    }
}
