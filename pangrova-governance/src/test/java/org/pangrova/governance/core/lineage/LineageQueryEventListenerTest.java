package org.pangrova.governance.core.lineage;

import org.pangrova.governance.lineage.provider.openlineage.MarquezTestBackend;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import org.pangrova.observability.api.event.QueryState;
import org.pangrova.observability.api.event.QueryCompletedEvent;
import org.pangrova.observability.api.event.QueryCreatedEvent;
import org.pangrova.observability.api.event.QueryLineageMetadata;
import org.pangrova.observability.api.event.QueryStatistics;
import org.pangrova.governance.lineage.provider.openlineage.OpenLineageHttpTransport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies the query SPI through actual SDK HTTP ingestion and Marquez's PostgreSQL journal. */
@Tag("integration")
class LineageQueryEventListenerTest {
    private static MarquezTestBackend backend;

    @BeforeAll
    static void startReferenceBackend() {
        backend = new MarquezTestBackend();
    }

    @AfterAll
    static void closeReferenceBackend() {
        if (backend != null) backend.close();
    }

    @Test
    void queryCompletedSuccessEmitsCompleteEvent() throws Exception {
        var emitter = new LineageEventEmitter(backend.transport());
        var listener = new LineageQueryEventListener(emitter);
        try {
            listener.queryCreated(new QueryCreatedEvent("q1", "SELECT 1", "alice", 1000));
            listener.queryCompleted(new QueryCompletedEvent(
                    "q1", "SELECT 1", "alice", QueryState.FINISHED,
                    1000, 1100, QueryStatistics.wallTimeOnly(100), Optional.empty()));
        } finally { emitter.close(); }
        assertDrained(emitter);
        var events = backend.events("q1");
        assertThat(count(events, "COMPLETE")).isEqualTo(1);
        assertThat(count(events, "FAIL")).isEqualTo(0);
        assertThat(count(events, "START")).isEqualTo(1);
        assertThat(events).hasSize(2);
    }

    @Test
    void queryCompletedFailureEmitsFailEvent() throws Exception {
        var emitter = new LineageEventEmitter(backend.transport());
        var listener = new LineageQueryEventListener(emitter);
        try {
            listener.queryCreated(new QueryCreatedEvent("q2", "SELECT bad", "bob", 2000));
            listener.queryCompleted(new QueryCompletedEvent(
                    "q2", "SELECT bad", "bob", QueryState.FAILED,
                    2000, 2050, QueryStatistics.wallTimeOnly(50), Optional.of("table not found")));
        } finally { emitter.close(); }
        assertDrained(emitter);
        var events = backend.events("q2");
        assertThat(count(events, "FAIL")).isEqualTo(1);
        assertThat(count(events, "COMPLETE")).isEqualTo(0);
        assertThat(count(events, "START")).isEqualTo(1);
        assertThat(events).hasSize(2);
        JsonNode terminal = events.stream().filter(event -> "FAIL".equals(event.path("eventType").asText()))
                .findFirst().orElseThrow();
        assertThat(terminal.at("/run/facets/errorMessage/message").asText()).isEqualTo("table not found");
        assertThat(terminal.at("/run/facets/errorMessage/programmingLanguage").asText()).isEqualTo("SQL");
    }

    @Test
    void queryCreatedEmitsOneStartAndNoTerminal() throws Exception {
        var emitter = new LineageEventEmitter(backend.transport());
        var listener = new LineageQueryEventListener(emitter);
        try { listener.queryCreated(new QueryCreatedEvent("q3", "SELECT 1", "carol", 3000)); }
        finally { emitter.close(); }
        assertDrained(emitter);
        var events = backend.events("q3");
        assertThat(events).hasSize(1);
        assertThat(count(events, "START")).isEqualTo(1);
        assertThat(count(events, "COMPLETE") + count(events, "FAIL") + count(events, "ABORT")).isZero();
        assertThat(events.get(0).at("/job/facets/sql/query").asText()).isEqualTo("SELECT 1");
        assertThat(events.get(0).at("/run/runId").asText())
                .isEqualTo(OpenLineageHttpTransport.normalizeRunId("q3").toString());
    }

    @Test
    void cancellationEmitsAbortRatherThanCompleteOrFail() throws Exception {
        var emitter = new LineageEventEmitter(backend.transport());
        var listener = new LineageQueryEventListener(emitter);
        try {
            listener.queryCreated(new QueryCreatedEvent("cancelled-query", "SELECT 1", "alice", 1000));
            listener.queryCompleted(new QueryCompletedEvent("cancelled-query", "SELECT 1", "alice",
                    QueryState.CANCELLED, 1000, 1100, QueryStatistics.wallTimeOnly(100), Optional.empty()));
        } finally { emitter.close(); }
        assertDrained(emitter);
        var events = backend.events("cancelled-query");
        assertThat(events).hasSize(2);
        assertThat(count(events, "START")).isEqualTo(1);
        assertThat(count(events, "ABORT")).isEqualTo(1);
        assertThat(count(events, "COMPLETE") + count(events, "FAIL")).isZero();
    }

    @Test
    void terminalCapturesDottedDatasetNamesAndActualDerivationKinds() throws Exception {
        String source = "catalog.schema.quoted.table";
        var input = new QueryLineageMetadata.Dataset("pangrova", source, List.of());
        var reference = new QueryLineageMetadata.SourceColumn("pangrova", source, "amount.with.dot");
        var output = new QueryLineageMetadata.Dataset("pangrova", "catalog.schema.target", List.of(
                new QueryLineageMetadata.Column("direct", List.of(reference),
                        QueryLineageMetadata.DerivationKind.DIRECT),
                new QueryLineageMetadata.Column("computed", List.of(reference),
                        QueryLineageMetadata.DerivationKind.TRANSFORM),
                new QueryLineageMetadata.Column("total", List.of(reference),
                        QueryLineageMetadata.DerivationKind.AGGREGATE)));
        var emitter = new LineageEventEmitter(backend.transport());
        var listener = new LineageQueryEventListener(emitter);
        try {
            listener.queryCreated(new QueryCreatedEvent(
                    "dataset-query", "INSERT INTO target SELECT amount", "alice", 1000));
            listener.queryCompleted(new QueryCompletedEvent("dataset-query", null, "alice", QueryState.FINISHED,
                    1000, 1100, QueryStatistics.wallTimeOnly(100), Optional.empty(),
                    new QueryLineageMetadata(List.of(input), List.of(output))));
        } finally { emitter.close(); }
        assertDrained(emitter);
        var events = backend.events("dataset-query");
        assertThat(events).hasSize(2);
        var terminal = events.stream().filter(event -> "COMPLETE".equals(event.path("eventType").asText()))
                .findFirst().orElseThrow();
        assertThat(terminal.at("/inputs/0/name").asText()).isEqualTo(source);
        assertThat(terminal.at("/outputs/0/name").asText()).isEqualTo("catalog.schema.target");
        var fields = terminal.at("/outputs/0/facets/columnLineage/fields");
        assertThat(fields.path("direct").path("transformationType").asText()).isEqualTo("IDENTITY");
        assertThat(fields.path("computed").path("transformationType").asText()).isEqualTo("TRANSFORMATION");
        assertThat(fields.path("total").path("transformationType").asText()).isEqualTo("AGGREGATION");
        assertThat(fields.at("/total/inputFields/0/name").asText()).isEqualTo(source);
        assertThat(fields.at("/total/inputFields/0/field").asText()).isEqualTo("amount.with.dot");
    }

    @Test
    void borrowedDispatcherDoesNotRetainAnotherManagersLineageListener() throws Exception {
        var parent = org.pangrova.observability.api.event.QueryEventDispatcher.of();
        var first = new LineageEventEmitter(backend.transport());
        var second = new LineageEventEmitter(backend.transport());
        var firstOwner = parent.withListener(new LineageQueryEventListener(first));
        var secondOwner = parent.withListener(new LineageQueryEventListener(second));
        try {
            firstOwner.fireQueryCreated(new QueryCreatedEvent("first-manager", "SELECT 1", "alice", 1000));
            secondOwner.fireQueryCreated(new QueryCreatedEvent("second-manager", "SELECT 2", "bob", 1000));
            firstOwner.fireQueryCompleted(new QueryCompletedEvent("first-manager", "SELECT 1", "alice",
                    QueryState.FINISHED, 1000, 1100, QueryStatistics.wallTimeOnly(100), Optional.empty()));
            secondOwner.fireQueryCompleted(new QueryCompletedEvent("second-manager", "SELECT 2", "bob",
                    QueryState.FINISHED, 1000, 1100, QueryStatistics.wallTimeOnly(100), Optional.empty()));
            assertThat(parent.isEmpty()).isTrue();
        } finally {
            first.close();
            second.close();
        }
        assertDrained(first);
        assertDrained(second);
        assertThat(backend.events("first-manager")).hasSize(2);
        assertThat(backend.events("second-manager")).hasSize(2);
    }

    private static long count(List<JsonNode> events, String type) {
        return events.stream().filter(event -> type.equals(event.path("eventType").asText())).count();
    }

    private static void assertDrained(LineageEventEmitter emitter) {
        assertThat(emitter.shutdownTimedOut()).isFalse();
        assertThat(emitter.eventsDroppedTotal()).isZero();
    }
}
