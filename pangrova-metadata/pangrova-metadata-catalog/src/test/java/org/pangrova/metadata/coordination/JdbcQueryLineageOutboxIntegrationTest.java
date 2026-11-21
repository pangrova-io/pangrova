package org.pangrova.metadata.coordination;

import org.pangrova.governance.api.enums.LineageEventType;
import org.pangrova.persistence.query.postgresql.state.JdbcQueryStateJournal;
import org.pangrova.persistence.query.postgresql.lineage.JdbcQueryLineageOutbox;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.pangrova.query.api.lineage.QueryLineageFact;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.query.api.lineage.QueryLineageMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Exercises real PostgreSQL transactions, row locks, permission failures, clocks and successor owners. */
class JdbcQueryLineageOutboxIntegrationTest {
    private DataSource dataSource;
    private JdbcQueryLineageOutbox outbox;

    @BeforeEach
    void migrateActualSchema() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("lineage_outbox");
        outbox = new JdbcQueryLineageOutbox(dataSource);
    }

    @Test
    void exactReplaySurvivesNewStoreAndAcknowledgement() {
        QueryLineageFact start = fact("q-replay", LineageEventType.START, 1000);
        outbox.append(start);
        new JdbcQueryLineageOutbox(dataSource).append(start);
        assertThat(outbox.pendingCount()).isEqualTo(1);
        var delivery = outbox.claim("pangrova-real-postgres", Duration.ofSeconds(5)).orElseThrow();
        assertThat(delivery.fact()).isEqualTo(start);
        assertThat(outbox.claim("pangrova-real-postgres", Duration.ofSeconds(5))).isEmpty();
        outbox.acknowledge(delivery);
        new JdbcQueryLineageOutbox(dataSource).acknowledge(delivery);
        new JdbcQueryLineageOutbox(dataSource).append(start);
        assertThat(outbox.pendingCount()).isZero();
    }

    @Test
    void conflictingLogicalIdentityNeverOverwritesTheFrozenFact() {
        QueryLineageFact original = fact("q-conflict", LineageEventType.COMPLETE, 2000);
        outbox.append(original);
        assertThatThrownBy(() -> outbox.append(fact("q-conflict", LineageEventType.FAIL, 2000)))
                .isInstanceOf(IllegalStateException.class).hasRootCauseInstanceOf(java.sql.SQLException.class);
        assertThat(outbox.pendingCount()).isEqualTo(1);
        outbox.append(fact("q-conflict", LineageEventType.START, 1000));
        var start = outbox.claim("pangrova-real-postgres", Duration.ofSeconds(5)).orElseThrow();
        outbox.acknowledge(start);
        assertThat(outbox.claim("pangrova-real-postgres", Duration.ofSeconds(5)).orElseThrow().fact()).isEqualTo(original);
    }

    @Test
    void terminalCannotOvertakeAnUnacknowledgedStart() {
        outbox.append(fact("q-order", LineageEventType.COMPLETE, 2000));
        assertThat(outbox.claim("pangrova-real-postgres", Duration.ofSeconds(5))).isEmpty();
        outbox.append(fact("q-order", LineageEventType.START, 1000));
        var start = outbox.claim("pangrova-real-postgres", Duration.ofSeconds(5)).orElseThrow();
        assertThat(start.fact().phase()).isEqualTo(LineageEventType.START);
        assertThat(outbox.claim("pangrova-real-postgres", Duration.ofSeconds(5))).isEmpty();
        outbox.release(start);
        var retry = outbox.claim("pangrova-real-postgres", Duration.ofSeconds(5)).orElseThrow();
        assertThat(retry.fact()).isEqualTo(start.fact());
        assertThat(retry.token()).isGreaterThan(start.token());
        outbox.acknowledge(retry);
        assertThat(outbox.claim("pangrova-real-postgres", Duration.ofSeconds(5)).orElseThrow().fact().phase())
                .isEqualTo(LineageEventType.COMPLETE);
    }

    @Test
    void actualExpiredLeaseCannotAcknowledgeSuccessorDelivery() throws Exception {
        outbox.append(fact("q-handoff", LineageEventType.START, 1000));
        var former = outbox.claim("pangrova-real-postgres", Duration.ofMillis(100)).orElseThrow();
        sql("SELECT pg_sleep(0.15)");
        var successor = new JdbcQueryLineageOutbox(dataSource).claim("pangrova-real-postgres", Duration.ofSeconds(5)).orElseThrow();
        assertThat(successor.token()).isGreaterThan(former.token());
        assertThatThrownBy(() -> outbox.acknowledge(former)).isInstanceOf(IllegalStateException.class);
        assertThat(outbox.pendingCount()).isEqualTo(1);
        outbox.acknowledge(successor);
        assertThat(outbox.pendingCount()).isZero();
    }

    @Test
    void clockIsReadAfterActualRowLockWaitBeforeAcknowledgement() throws Exception {
        outbox.append(fact("q-lock-clock", LineageEventType.START, 1000));
        var delivery = outbox.claim("pangrova-real-postgres", Duration.ofMillis(500)).orElseThrow();
        var executor = Executors.newSingleThreadExecutor();
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (var lock = connection.prepareStatement(
                    "SELECT event_id FROM query_lineage_outbox WHERE event_id = ? FOR UPDATE")) {
                lock.setObject(1, delivery.fact().eventId());
                try (var rows = lock.executeQuery()) { assertThat(rows.next()).isTrue(); }
            }
            var acknowledgement = executor.submit(() -> outbox.acknowledge(delivery));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(blockedAcknowledgements()).isPositive());
            try (var clock = connection.createStatement()) { clock.execute("SELECT pg_sleep(0.6)"); }
            connection.commit();
            assertThatThrownBy(() -> acknowledgement.get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(java.sql.SQLException.class);
            assertThat(outbox.pendingCount()).isEqualTo(1);
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void queryJournalCommitAndOutboxAreAtomicAndPurgeKeepsDeliveryFacts() throws Exception {
        QueryLineageFact start = fact("q-atomic", LineageEventType.START, 1000);
        var event = new QueryStateEvent(JournalEventContext.create(1000, "owner"),
                start.queryId(), start.sql(), "{}", "QUEUED", start);
        try (var journal = new JdbcQueryStateJournal(dataSource,
                new StateJournalConfig("JDBC", SharedPostgresContainer.jdbcUrl(), 10, 20, 24, 50_000))) {
            journal.appendQueryEventSync(event);
            try (var recovered = new JdbcQueryStateJournal(dataSource,
                    new StateJournalConfig("JDBC", SharedPostgresContainer.jdbcUrl(), 10, 20, 24, 50_000))) {
                assertThat(recovered.latestQueryEvent(start.queryId())).contains(event);
            }
            assertThat(outbox.pendingCount()).isEqualTo(1);
            journal.purgeQuery(start.queryId());
            assertThat(journal.latestQueryEvent(start.queryId())).isEmpty();
            assertThat(outbox.pendingCount()).isEqualTo(1);
        }
    }

    @Test
    void actualOutboxInsertDenialRollsBackJournalAndRetryCommitsBoth() throws Exception {
        sql("""
                CREATE FUNCTION deny_lineage_insert() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'Actual outbox insert denied' USING ERRCODE='42501'; END $$
                """);
        sql("CREATE TRIGGER deny_lineage BEFORE INSERT ON query_lineage_outbox "
                + "FOR EACH ROW EXECUTE FUNCTION deny_lineage_insert()");
        QueryLineageFact start = fact("q-permission", LineageEventType.START, 1000);
        var event = new QueryStateEvent(JournalEventContext.create(1000, "owner"),
                start.queryId(), start.sql(), "{}", "QUEUED", start);
        try (var journal = new JdbcQueryStateJournal(dataSource,
                new StateJournalConfig("JDBC", SharedPostgresContainer.jdbcUrl(), 50, 20, 24, 50_000))) {
            try {
                assertThatThrownBy(() -> journal.appendQueryEventSync(event)).isInstanceOf(JournalWriteException.class);
                assertThat(count("query_state_journal")).isZero();
                assertThat(outbox.pendingCount()).isZero();
            } finally {
                sql("DROP TRIGGER deny_lineage ON query_lineage_outbox");
            }
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(journal.latestQueryEvent(start.queryId())).contains(event);
                assertThat(outbox.pendingCount()).isEqualTo(1);
            });
            journal.appendQueryEventSync(event);
            assertThat(count("query_state_journal")).isEqualTo(1);
            assertThat(outbox.pendingCount()).isEqualTo(1);
        }
    }

    @Test
    void anotherDeploymentCannotClaimOrRelabelFrozenNamespace() {
        QueryLineageFact own = fact("q-namespace", LineageEventType.START, 1000);
        var other = new QueryLineageFact("another-deployment", "q-other", own.phase(), 999,
                own.sql(), own.failureMessage(), own.lineage());
        outbox.append(other);
        outbox.append(own);
        var delivery = outbox.claim(own.namespace(), Duration.ofSeconds(5)).orElseThrow();
        assertThat(delivery.fact()).isEqualTo(own);
        outbox.acknowledge(delivery);
        assertThat(outbox.claim(own.namespace(), Duration.ofSeconds(5))).isEmpty();
        assertThat(outbox.pendingCount()).isEqualTo(1);
        assertThat(outbox.claim(other.namespace(), Duration.ofSeconds(5)).orElseThrow().fact()).isEqualTo(other);
    }

    @Test
    void actualStoreRetainsMoreThanTheFormerResidentQueueCapacity() {
        for (int index = 0; index < 1005; index++) {
            outbox.append(fact("q-capacity-" + index, LineageEventType.START, 1000 + index));
        }
        assertThat(new JdbcQueryLineageOutbox(dataSource).pendingCount()).isEqualTo(1005);
    }

    private static QueryLineageFact fact(String queryId, LineageEventType phase, long time) {
        return new QueryLineageFact("pangrova-real-postgres", queryId, phase, time,
                phase == LineageEventType.START ? "SELECT amount FROM catalog.schema.source" : null,
                phase == LineageEventType.FAIL ? "actual terminal failure" : null,
                new QueryLineageMetadata(List.of(new QueryLineageMetadata.Dataset("pangrova",
                        "catalog.schema.source", List.of())), List.of()));
    }

    private void sql(String sql) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private long count(String table) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT count(*) FROM " + table)) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private long blockedAcknowledgements() throws Exception {
        try (var connection = dataSource.getConnection(); var query = connection.prepareStatement("""
                SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'
                    AND query LIKE '%payload::text, delivery_token, lease_until, delivered_at%'
                """); var rows = query.executeQuery()) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }
}
