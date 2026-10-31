package org.pangrova.query.store.jdbc.event;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.pangrova.observability.api.event.QueryState;
import org.pangrova.observability.api.event.QueryCompletedEvent;
import org.pangrova.observability.api.event.QueryCreatedEvent;
import org.pangrova.observability.api.event.QueryEventListener;
import org.pangrova.observability.api.event.QueryStatistics;
import org.pangrova.observability.api.event.SplitCompletedEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.observability.api.event.QueryEventDispatcher;

/**
 * Validates the durable EventListener contract (#181): a completed query fired through
 * {@link JdbcQueryEventListener} is persisted to {@code query_events} and read back
 * across a fresh connection, and {@link QueryEventDispatcher} fault-isolates listeners.
 * Also validates per-query profile persistence (#180) via split profiles.
 */
class JdbcQueryEventListenerTest {

    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("pangrova_test")
            .withUsername("pangrova")
            .withPassword("pangrova_test");

    @BeforeAll
    static void startContainer() { PG.start(); }

    @AfterAll
    static void stopContainer() { PG.stop(); }

    private DataSource dataSource;
    private JdbcQueryEventListener listener;

    @BeforeEach
    void setUp() throws Exception {
        org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        // Drop tables so each test starts with a clean schema
        try (var conn = ds.getConnection()) {
            conn.createStatement().execute("DROP TABLE IF EXISTS query_split_profiles");
            conn.createStatement().execute("DROP TABLE IF EXISTS query_events");
        }
        this.dataSource = ds;
        createSchema(ds);
        this.listener = new JdbcQueryEventListener(ds);
    }

    private static void createSchema(DataSource ds) throws Exception {
        try (var conn = ds.getConnection(); var stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE query_events (
                        query_id        VARCHAR(128) PRIMARY KEY,
                        sql_text        TEXT,
                        principal       VARCHAR(256),
                        final_state     VARCHAR(32) NOT NULL,
                        create_time     TIMESTAMP,
                        end_time        TIMESTAMP,
                        wall_time_ms    BIGINT,
                        cpu_time_ms     BIGINT,
                        total_rows      BIGINT,
                        total_bytes     BIGINT,
                        failure_message TEXT,
                        success         BOOLEAN
                    )
                    """);
            stmt.execute("""
                    CREATE TABLE query_split_profiles (
                        query_id            VARCHAR(128) NOT NULL,
                        stage_id            VARCHAR(128) NOT NULL,
                        split_id            VARCHAR(256) NOT NULL,
                        catalog_name        VARCHAR(256),
                        start_time          TIMESTAMP,
                        end_time            TIMESTAMP,
                        duration_ms         BIGINT,
                        processed_rows      BIGINT,
                        processed_bytes     BIGINT,
                        failure_message     TEXT,
                        success             BOOLEAN,
                        PRIMARY KEY (query_id, stage_id, split_id)
                    )
                    """);
        }
    }

    @Test
    void persistsAndReadsBackCompletedEvent() {
        QueryStatistics stats = new QueryStatistics(120L, 80L, 1_000L, 4_096L);
        QueryCompletedEvent event = new QueryCompletedEvent(
                "q-1", "SELECT 1", "alice", QueryState.FINISHED,
                1_000L, 1_120L, stats, Optional.empty());

        listener.queryCompleted(event);

        // Read back through a fresh listener over the same data source to prove durability.
        Optional<QueryCompletedEvent> read =
                new JdbcQueryEventListener(dataSource).findByQueryId("q-1");
        assertThat(read).isPresent();
        QueryCompletedEvent e = read.get();
        assertThat(e.queryId()).isEqualTo("q-1");
        assertThat(e.sql()).isEqualTo("SELECT 1");
        assertThat(e.principal()).isEqualTo("alice");
        assertThat(e.finalState()).isEqualTo(QueryState.FINISHED);
        assertThat(e.succeeded()).isTrue();
        assertThat(e.statistics().wallTimeMillis()).isEqualTo(120L);
        assertThat(e.statistics().cpuTimeMillis()).isEqualTo(80L);
        assertThat(e.statistics().totalRows()).isEqualTo(1_000L);
        assertThat(e.statistics().totalBytes()).isEqualTo(4_096L);
        assertThat(e.failureMessage()).isEmpty();
    }

    @Test
    void persistsFailureMessage() {
        QueryCompletedEvent event = new QueryCompletedEvent(
                "q-fail", "SELECT bad", "bob", QueryState.FAILED,
                2_000L, 2_050L, QueryStatistics.wallTimeOnly(50L),
                Optional.of("table not found"));

        listener.queryCompleted(event);

        QueryCompletedEvent e = listener.findByQueryId("q-fail").orElseThrow();
        assertThat(e.succeeded()).isFalse();
        assertThat(e.finalState()).isEqualTo(QueryState.FAILED);
        assertThat(e.failureMessage()).contains("table not found");
    }

    @Test
    void cancellationRemainsDistinctAcrossFreshConnection() {
        QueryCompletedEvent event = new QueryCompletedEvent(
                "q-cancel", "SELECT 1", "alice", QueryState.CANCELLED,
                2_000L, 2_050L, QueryStatistics.wallTimeOnly(50L), Optional.empty());
        listener.queryCompleted(event);
        QueryCompletedEvent restored = new JdbcQueryEventListener(dataSource)
                .findByQueryId("q-cancel").orElseThrow();
        assertThat(restored.finalState()).isEqualTo(QueryState.CANCELLED);
        assertThat(restored.succeeded()).isFalse();
        assertThat(restored.failureMessage()).isEmpty();
    }

    @Test
    void findByQueryIdReturnsEmptyWhenAbsent() {
        assertThat(listener.findByQueryId("missing")).isEmpty();
    }

    @Test
    void dispatcherFaultIsolatesThrowingListener() {
        JdbcQueryEventListener persisting = this.listener;
        QueryEventListener throwing = new QueryEventListener() {
            @Override
            public void queryCompleted(QueryCompletedEvent event) {
                throw new IllegalStateException("boom");
            }
        };
        // Throwing listener registered first must not suppress the persisting one.
        QueryEventDispatcher dispatcher = QueryEventDispatcher.of(throwing, persisting);

        QueryCompletedEvent event = new QueryCompletedEvent(
                "q-iso", "SELECT 2", "carol", QueryState.FINISHED,
                3_000L, 3_010L, QueryStatistics.wallTimeOnly(10L), Optional.empty());
        dispatcher.fireQueryCompleted(event);

        assertThat(persisting.findByQueryId("q-iso")).isPresent();
    }

    @Test
    void dispatcherIsEmptyReportsRegistration() {
        assertThat(new QueryEventDispatcher(List.of()).isEmpty()).isTrue();
        QueryEventDispatcher d = new QueryEventDispatcher(List.of());
        d.register(new QueryEventListener() { });
        assertThat(d.isEmpty()).isFalse();
    }

    @Test
    void dispatcherFiresCreatedEvent() {
        QueryCreatedEvent[] captured = new QueryCreatedEvent[1];
        QueryEventListener capturing = new QueryEventListener() {
            @Override
            public void queryCreated(QueryCreatedEvent event) {
                captured[0] = event;
            }
        };
        QueryEventDispatcher.of(capturing).fireQueryCreated(
                new QueryCreatedEvent("q-c", "SELECT 3", "dave", 4_000L));
        assertThat(captured[0]).isNotNull();
        assertThat(captured[0].queryId()).isEqualTo("q-c");
        assertThat(captured[0].principal()).isEqualTo("dave");
    }

    // ── Split profile persistence tests (Task 180) ──

    @Test
    void persistsAndReadsBackSplitProfile() {
        SplitCompletedEvent split = new SplitCompletedEvent(
                "q-split-1", "stage-0", "split-0", "iceberg",
                1_000L, 1_050L, 500L, 4_096L, Optional.empty());

        listener.splitCompleted(split);

        List<SplitCompletedEvent> splits = listener.findSplitsByQueryId("q-split-1");
        assertThat(splits).hasSize(1);
        SplitCompletedEvent s = splits.get(0);
        assertThat(s.queryId()).isEqualTo("q-split-1");
        assertThat(s.stageId()).isEqualTo("stage-0");
        assertThat(s.splitId()).isEqualTo("split-0");
        assertThat(s.catalogName()).isEqualTo("iceberg");
        assertThat(s.processedRows()).isEqualTo(500L);
        assertThat(s.processedBytes()).isEqualTo(4_096L);
        assertThat(s.succeeded()).isTrue();
        assertThat(s.failureMessage()).isEmpty();
    }

    @Test
    void persistsMultipleSplitsForSameQuery() {
        listener.splitCompleted(new SplitCompletedEvent(
                "q-multi", "stage-0", "split-0", "iceberg",
                1_000L, 1_050L, 100L, 1_024L, Optional.empty()));
        listener.splitCompleted(new SplitCompletedEvent(
                "q-multi", "stage-0", "split-1", "iceberg",
                1_050L, 1_100L, 200L, 2_048L, Optional.empty()));
        listener.splitCompleted(new SplitCompletedEvent(
                "q-multi", "stage-1", "split-0", "paimon",
                1_100L, 1_200L, 300L, 3_072L, Optional.of("timeout")));

        List<SplitCompletedEvent> splits = listener.findSplitsByQueryId("q-multi");
        assertThat(splits).hasSize(3);
        // Ordered by stage_id, split_id
        assertThat(splits.get(0).stageId()).isEqualTo("stage-0");
        assertThat(splits.get(0).splitId()).isEqualTo("split-0");
        assertThat(splits.get(2).succeeded()).isFalse();
        assertThat(splits.get(2).failureMessage()).contains("timeout");
    }

    @Test
    void findSplitsReturnsEmptyWhenNoSplits() {
        assertThat(listener.findSplitsByQueryId("nonexistent")).isEmpty();
    }

    // ── Split completed dispatcher test (Task 181) ──

    @Test
    void dispatcherFiresSplitCompletedEvent() {
        SplitCompletedEvent[] captured = new SplitCompletedEvent[1];
        QueryEventListener capturing = new QueryEventListener() {
            @Override
            public void splitCompleted(SplitCompletedEvent event) {
                captured[0] = event;
            }
        };
        QueryEventDispatcher.of(capturing).fireSplitCompleted(
                new SplitCompletedEvent("q-s", "stage-0", "split-0", "iceberg",
                        1_000L, 1_050L, 500L, 4_096L, Optional.empty()));
        assertThat(captured[0]).isNotNull();
        assertThat(captured[0].queryId()).isEqualTo("q-s");
        assertThat(captured[0].splitId()).isEqualTo("split-0");
    }

    @Test
    void dispatcherFaultIsolatesSplitCompletedThrowingListener() {
        SplitCompletedEvent[] captured = new SplitCompletedEvent[1];
        QueryEventListener throwing = new QueryEventListener() {
            @Override
            public void splitCompleted(SplitCompletedEvent event) {
                throw new IllegalStateException("boom");
            }
        };
        QueryEventListener capturing = new QueryEventListener() {
            @Override
            public void splitCompleted(SplitCompletedEvent event) {
                captured[0] = event;
            }
        };
        QueryEventDispatcher dispatcher = QueryEventDispatcher.of(throwing, capturing);
        dispatcher.fireSplitCompleted(
                new SplitCompletedEvent("q-iso-s", "s0", "sp1", "iceberg",
                        1000, 1050, 500, 2048, Optional.empty()));
        assertThat(captured[0]).isNotNull();
        assertThat(captured[0].queryId()).isEqualTo("q-iso-s");
    }

    // ── Security denial routing test (Task 181) ──

    @Test
    void dispatcherFiresSecurityDeniedAsFailedQueryEvent() {
        QueryCompletedEvent[] captured = new QueryCompletedEvent[1];
        QueryEventListener capturing = new QueryEventListener() {
            @Override
            public void queryCompleted(QueryCompletedEvent event) {
                captured[0] = event;
            }
        };
        QueryEventDispatcher dispatcher = QueryEventDispatcher.of(capturing);
        dispatcher.fireSecurityDenied("q-denied", "alice", "Table access denied");

        assertThat(captured[0]).isNotNull();
        assertThat(captured[0].queryId()).isEqualTo("q-denied");
        assertThat(captured[0].principal()).isEqualTo("alice");
        assertThat(captured[0].succeeded()).isFalse();
        assertThat(captured[0].failureMessage()).contains("Table access denied");
    }

    // ── Full profile persistence end-to-end test (Task 180) ──

    @Test
    void fullQueryProfilePersistence() {
        // Persist a query event + its split profiles (simulates post-mortem replay data)
        QueryStatistics stats = new QueryStatistics(200L, 150L, 5_000L, 20_480L);
        QueryCompletedEvent queryEvent = new QueryCompletedEvent(
                "q-profile", "SELECT * FROM orders", "alice",
                QueryState.FINISHED, 1_000L, 1_200L, stats, Optional.empty());
        listener.queryCompleted(queryEvent);

        listener.splitCompleted(new SplitCompletedEvent(
                "q-profile", "stage-0", "split-0", "iceberg",
                1_000L, 1_100L, 2_500L, 10_240L, Optional.empty()));
        listener.splitCompleted(new SplitCompletedEvent(
                "q-profile", "stage-0", "split-1", "iceberg",
                1_050L, 1_150L, 2_500L, 10_240L, Optional.empty()));

        // Read back the full profile
        Optional<QueryCompletedEvent> queryRead = listener.findByQueryId("q-profile");
        assertThat(queryRead).isPresent();
        assertThat(queryRead.get().statistics().wallTimeMillis()).isEqualTo(200L);
        assertThat(queryRead.get().statistics().totalRows()).isEqualTo(5_000L);

        List<SplitCompletedEvent> splitRead = listener.findSplitsByQueryId("q-profile");
        assertThat(splitRead).hasSize(2);
        long totalSplitRows = splitRead.stream().mapToLong(SplitCompletedEvent::processedRows).sum();
        assertThat(totalSplitRows).isEqualTo(5_000L);
    }
}
