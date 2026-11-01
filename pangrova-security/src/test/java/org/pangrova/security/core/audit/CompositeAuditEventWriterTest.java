package org.pangrova.security.core.audit;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.spi.security.audit.AuditEvent;
import org.pangrova.spi.security.audit.AuditEventQuery;
import org.pangrova.spi.security.audit.AuditEventType;
import org.pangrova.spi.security.audit.AuditEventWriter;
import org.pangrova.persistence.api.security.audit.QueryableAuditEventWriter;

/**
 * Tests for {@link CompositeAuditEventWriter}: multi-sink fan-out, fault isolation
 * between sinks, and query delegation to queryable delegate.
 */
class CompositeAuditEventWriterTest {

    @Test
    void fansOutToAllDelegates() {
        var sink1 = new CopyOnWriteArrayList<AuditEvent>();
        var sink2 = new CopyOnWriteArrayList<AuditEvent>();

        CompositeAuditEventWriter composite = new CompositeAuditEventWriter(List.of(
                sink1::add,
                sink2::add));

        AuditEvent event = createEvent("e-1");
        composite.write(event);

        assertThat(sink1).hasSize(1);
        assertThat(sink2).hasSize(1);
        assertThat(sink1.get(0).eventId()).isEqualTo("e-1");
        assertThat(sink2.get(0).eventId()).isEqualTo("e-1");
    }

    @Test
    void isolatesPartialSinkFailure() {
        var successSink = new CopyOnWriteArrayList<AuditEvent>();
        AuditEventWriter failingSink = event -> {
            throw new RuntimeException("DB down");
        };

        CompositeAuditEventWriter composite = new CompositeAuditEventWriter(List.of(
                failingSink,
                successSink::add));

        AuditEvent event = createEvent("e-partial");
        // Should NOT throw because one sink succeeded
        composite.write(event);

        assertThat(successSink).hasSize(1);
    }

    @Test
    void propagatesWhenAllSinksFail() {
        AuditEventWriter failing1 = event -> {
            throw new RuntimeException("sink1 down");
        };
        AuditEventWriter failing2 = event -> {
            throw new RuntimeException("sink2 down");
        };

        CompositeAuditEventWriter composite = new CompositeAuditEventWriter(List.of(
                failing1, failing2));

        assertThatThrownBy(() -> composite.write(createEvent("e-allfail")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("sink1 down");
    }

    @Test
    void delegatesQueryToFirstQueryableSink() {
        var received = new CopyOnWriteArrayList<AuditEvent>();
        AuditEventWriter logSink = received::add;
        QueryableAuditEventWriter queryableSink = new QueryableAuditEventWriter() {
            private final List<AuditEvent> store = new CopyOnWriteArrayList<>();

            @Override
            public void write(AuditEvent event) {
                store.add(event);
            }

            @Override
            public List<AuditEvent> query(AuditEventQuery query) {
                return store.stream()
                        .filter(e -> query.principal() == null
                                || e.principal().equals(query.principal()))
                        .toList();
            }
        };

        CompositeAuditEventWriter composite = new CompositeAuditEventWriter(List.of(
                logSink, queryableSink));

        composite.write(createEvent("e-q1"));
        composite.write(createEvent("e-q2"));

        List<AuditEvent> results = composite.query(AuditEventQuery.byPrincipal("user"));
        assertThat(results).hasSize(2);
    }

    @Test
    void queryReturnsEmptyWhenNoQueryableDelegateExists() {
        CompositeAuditEventWriter composite = new CompositeAuditEventWriter(List.of(
                event -> {}));

        List<AuditEvent> results = composite.query(AuditEventQuery.byPrincipal("anyone"));
        assertThat(results).isEmpty();
    }

    @Test
    void rejectsEmptyDelegateList() {
        assertThatThrownBy(() -> new CompositeAuditEventWriter(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullDelegateList() {
        assertThatThrownBy(() -> new CompositeAuditEventWriter(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullElementInDelegateList() {
        List<AuditEventWriter> withNull = new java.util.ArrayList<>();
        withNull.add(event -> {});
        withNull.add(null);
        assertThatThrownBy(() -> new CompositeAuditEventWriter(withNull))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void exposesdelegates() {
        AuditEventWriter w1 = event -> {};
        AuditEventWriter w2 = event -> {};
        CompositeAuditEventWriter composite = new CompositeAuditEventWriter(List.of(w1, w2));
        assertThat(composite.delegates()).hasSize(2);
    }

    private static AuditEvent createEvent(String id) {
        return new AuditEvent(id, AuditEventType.QUERY_SUBMITTED, Instant.now(),
                "user", "127.0.0.1", "t", "s", "q", "SELECT 1", "c", "sch",
                List.of(), true, null, 0, 0, 0, Map.of());
    }
}
