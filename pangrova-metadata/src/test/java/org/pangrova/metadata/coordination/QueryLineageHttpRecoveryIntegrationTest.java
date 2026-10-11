package org.pangrova.metadata.coordination;

import io.openlineage.client.transports.HttpConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.governance.lineage.DatasetRef;
import org.pangrova.governance.lineage.LineageEvent;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.governance.lineage.OpenLineageHttpTransport;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.coordination.QueryLineageFact;
import org.pangrova.spi.execution.eventlistener.QueryLineageMetadata;
import org.pangrova.testing.lineage.ReferenceLineageBackend;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real SDK HTTP, PostgreSQL lock/acknowledgement and successor delivery; no replacement backend. */
class QueryLineageHttpRecoveryIntegrationTest {
    private static final int FORMAL_HTTP_TIMEOUT_MS = 5000;
    private static final int STARTUP_HTTP_TIMEOUT_MS = 20_000;
    private static ReferenceLineageBackend reference;
    private DataSource dataSource;
    private JdbcQueryLineageOutbox outbox;

    @BeforeAll
    static void startReferenceBackend() throws Exception {
        reference = new ReferenceLineageBackend();
        try {
            String id = UUID.randomUUID().toString();
            var source = new DatasetRef.SourceColumnRef(reference.namespace(), "warm.source", "amount");
            var columns = java.util.Arrays.stream(QueryLineageMetadata.DerivationKind.values())
                    .map(kind -> new DatasetRef.ColumnLineage(kind.name(), List.of(source), kind)).toList();
            try (var transport = transport(null, STARTUP_HTTP_TIMEOUT_MS)) {
                transport.send(new LineageEvent(LineageEvent.EventType.START, id, "fixture-readiness", "SELECT 1",
                        List.of(new DatasetRef(reference.namespace(), "warm.source", List.of())),
                        List.of(new DatasetRef(reference.namespace(), "warm.target", columns)),
                        Map.of(), Instant.now()));
            }
            assertThat(reference.events(UUID.fromString(id))).hasSize(1);
        } catch (Exception | Error failure) {
            try { reference.close(); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    @AfterAll
    static void closeReferenceBackend() {
        if (reference != null) reference.close();
    }

    @BeforeEach
    void createActualOutbox() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("lineage_http");
        outbox = new JdbcQueryLineageOutbox(dataSource);
    }

    @Test
    void successorDeliversFrozenStartBeforeTerminalAndReplaysDoNotCreateNewLogicalFacts() throws Exception {
        String run = UUID.randomUUID().toString();
        QueryLineageFact start = fact(run, QueryLineageFact.Phase.START, 1000);
        QueryLineageFact complete = fact(run, QueryLineageFact.Phase.COMPLETE, 1001);
        outbox.append(complete);
        outbox.append(start);
        new JdbcQueryLineageOutbox(dataSource).append(start);
        try (var owner = new LineageEventEmitter(transport(null, FORMAL_HTTP_TIMEOUT_MS),
                new JdbcQueryLineageOutbox(dataSource), reference.namespace())) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(outbox.pendingCount()).isZero());
            var actual = reference.events(UUID.fromString(run));
            assertThat(actual).hasSize(2);
            assertThat(actual.get(0).path("eventType").asText()).isEqualTo("START");
            assertThat(actual.get(1).path("eventType").asText()).isEqualTo("COMPLETE");
            assertThat(Instant.parse(actual.get(0).path("eventTime").asText()).toEpochMilli()).isEqualTo(1000);
            assertThat(actual.get(1).at("/outputs/0/facets/columnLineage/fields/total/inputFields/0/field")
                    .asText()).isEqualTo("amount");
            owner.record(start);
            owner.record(complete);
            assertThat(outbox.pendingCount()).isZero();
        }
    }

    @Test
    void actualHttpNonAcceptanceRetainsBothFactsForSuccessor() throws Exception {
        String run = UUID.randomUUID().toString();
        outbox.append(fact(run, QueryLineageFact.Phase.START, 1000));
        outbox.append(fact(run, QueryLineageFact.Phase.FAIL, 1001));
        try (var owner = new LineageEventEmitter(transport("/api/v1/unsupported-ingestion", FORMAL_HTTP_TIMEOUT_MS),
                outbox, reference.namespace())) {
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(deliveryAttempts()).isPositive());
            assertThat(outbox.pendingCount()).isEqualTo(2);
            assertThat(reference.events(UUID.fromString(run))).isEmpty();
        }
        try (var successor = new LineageEventEmitter(transport(null, FORMAL_HTTP_TIMEOUT_MS),
                new JdbcQueryLineageOutbox(dataSource), reference.namespace())) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(outbox.pendingCount()).isZero());
            assertThat(reference.events(UUID.fromString(run))).extracting(event -> event.path("eventType").asText())
                    .containsExactly("START", "FAIL");
        }
    }

    @Test
    void timedOutCloseCannotEraseEnteredHttpAndRepeatedCloseWaitsForPhysicalOwner() throws Exception {
        String run = UUID.randomUUID().toString();
        QueryLineageFact start = fact(run, QueryLineageFact.Phase.START, 1000);
        QueryLineageFact abort = fact(run, QueryLineageFact.Phase.ABORT, 1001);
        var owner = new LineageEventEmitter(transport(null, 1000), outbox, reference.namespace(), 100);
        try {
            try (var lock = reference.connection(); var statement = lock.createStatement()) {
                lock.setAutoCommit(false);
                statement.execute("LOCK TABLE lineage_events IN ACCESS EXCLUSIVE MODE");
                try {
                    owner.record(start);
                    owner.record(abort);
                    await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(actualBlockedIngestion()).isTrue());
                    owner.close();
                    assertThat(owner.shutdownTimedOut()).isTrue();
                    assertThat(outbox.pendingCount()).isEqualTo(2);
                    assertThatThrownBy(() -> owner.record(start)).isInstanceOf(IllegalStateException.class);
                    await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                        owner.close();
                        assertThat(owner.shutdownTimedOut()).isFalse();
                    });
                    assertThat(outbox.pendingCount()).isEqualTo(2);
                } finally {
                    lock.rollback();
                }
            }
            try (var successor = new LineageEventEmitter(transport(null, FORMAL_HTTP_TIMEOUT_MS),
                    new JdbcQueryLineageOutbox(dataSource), reference.namespace())) {
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(outbox.pendingCount()).isZero());
                var events = reference.events(UUID.fromString(run));
                assertThat(events).filteredOn(event -> "ABORT".equals(event.path("eventType").asText())).hasSize(1);
                assertThat(events).filteredOn(event -> "START".equals(event.path("eventType").asText())).isNotEmpty()
                        .allSatisfy(event -> assertThat(Instant.parse(event.path("eventTime").asText()).toEpochMilli())
                                .isEqualTo(start.timestampMs()));
            }
        } finally {
            owner.close();
        }
    }

    @Test
    void governanceContextCloseRetainsPendingHttpOwnerUntilRepeatedCloseJoins() throws Exception {
        closeActualGovernanceOwner(false);
    }

    @Test
    void governanceWiringClosePropagatesPendingHttpOwnerUntilRepeatedCloseJoins() throws Exception {
        closeActualGovernanceOwner(true);
    }

    private void closeActualGovernanceOwner(boolean legacyWrapper) throws Exception {
        String run = UUID.randomUUID().toString();
        var emitter = new LineageEventEmitter(transport(null, 1000), outbox, reference.namespace(), 100);
        var store = new org.pangrova.metadata.governance.JdbcQualityRuleStore(dataSource);
        store.initSchema();
        var handler = new org.pangrova.governance.quality.rule.GovernanceStatementHandler(store,
                new org.pangrova.governance.quality.rule.DefaultQualityRuleExecutor(),
                new org.pangrova.governance.quality.alert.DataQualityAlerter(
                        new org.pangrova.governance.quality.alert.AlertChannelRegistry()));
        var scheduler = new org.pangrova.governance.quality.schedule.CronScheduler(handler);
        var context = new org.pangrova.governance.GovernanceContext(
                new org.pangrova.governance.GovernanceDeps(emitter, handler, null, scheduler, store));
        AutoCloseable owner = legacyWrapper ? new org.pangrova.governance.GovernanceWiring(context) : context;
        try {
            try (var lock = reference.connection(); var statement = lock.createStatement()) {
                lock.setAutoCommit(false);
                statement.execute("LOCK TABLE lineage_events IN ACCESS EXCLUSIVE MODE");
                try {
                    emitter.record(fact(run, QueryLineageFact.Phase.START, 1000));
                    await().atMost(Duration.ofSeconds(5)).untilAsserted(
                            () -> assertThat(actualBlockedIngestion()).isTrue());
                    assertThatThrownBy(owner::close).isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("has not actually terminated");
                    assertThat(emitter.shutdownTimedOut()).isTrue();
                    assertThat(outbox.pendingCount()).isEqualTo(1);
                    assertThatThrownBy(() -> emitter.record(fact(run, QueryLineageFact.Phase.START, 1000)))
                            .isInstanceOf(IllegalStateException.class);
                    await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                        org.assertj.core.api.Assertions.assertThatCode(owner::close).doesNotThrowAnyException();
                        assertThat(emitter.shutdownTimedOut()).isFalse();
                    });
                    assertThat(outbox.pendingCount()).isEqualTo(1);
                } finally {
                    lock.rollback();
                }
            }
        } finally {
            owner.close();
        }
    }

    private long deliveryAttempts() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT COALESCE(SUM(delivery_token),0) FROM query_lineage_outbox")) {
            if (!rows.next()) throw new IllegalStateException("Actual delivery rows could not be read");
            return rows.getLong(1);
        }
    }

    private boolean actualBlockedIngestion() throws Exception {
        try (var connection = reference.connection(); var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                     + "WHERE wait_event_type='Lock' AND query LIKE '%lineage_events%')")) {
            if (!rows.next()) throw new IllegalStateException("Actual ingestion lock state could not be read");
            return rows.getBoolean(1);
        }
    }

    private static QueryLineageFact fact(String run, QueryLineageFact.Phase phase, long timestamp) {
        var source = new QueryLineageMetadata.SourceColumn(reference.namespace(), "source.orders", "amount");
        var column = new QueryLineageMetadata.Column("total", List.of(source),
                QueryLineageMetadata.DerivationKind.AGGREGATE);
        var metadata = new QueryLineageMetadata(
                List.of(new QueryLineageMetadata.Dataset(reference.namespace(), "source.orders", List.of())),
                List.of(new QueryLineageMetadata.Dataset(reference.namespace(), "target.totals", List.of(column))));
        return new QueryLineageFact(reference.namespace(), run, phase, timestamp,
                phase == QueryLineageFact.Phase.START ? "INSERT INTO target.totals SELECT SUM(amount) FROM source.orders" : null,
                phase == QueryLineageFact.Phase.FAIL ? "actual query failure" : null, metadata);
    }

    private static OpenLineageHttpTransport transport(String endpoint, int timeoutMs) {
        var config = new HttpConfig();
        config.setUrl(reference.endpoint());
        config.setEndpoint(endpoint);
        config.setTimeoutInMillis(timeoutMs);
        return new OpenLineageHttpTransport(config, reference.namespace());
    }
}
