package org.pangrova.persistence.query.postgresql.audit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

/**
 * The in-memory audit store silently drops entries at its size cap and loses all
 * history on restart. These tests pin the durable replacement against the real
 * migrated schema ({@code V7__query_audit_records.sql}).
 */
class JdbcQueryAuditStoreTest extends ContainerTestBase {

    private static QueryAuditRecord record(String queryId, Instant completedAt) {
        return new QueryAuditRecord(
                queryId, "user-1", "SELECT 1", "tenant-1", 2, 5,
                10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L, 100L,
                completedAt, true);
    }

    @Test
    void saveThenFindRoundTripsEveryField() throws Exception {
        DataSource source = MigratedPostgres.create();
        var store = new JdbcQueryAuditStore(source);
        Instant completedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        store.save(record("q-roundtrip", completedAt));

        var loaded = store.findById("q-roundtrip");
        assertThat(loaded).isPresent();
        var actual = loaded.orElseThrow();
        assertThat(actual.queryId()).isEqualTo("q-roundtrip");
        assertThat(actual.userId()).isEqualTo("user-1");
        assertThat(actual.sql()).isEqualTo("SELECT 1");
        assertThat(actual.tenantId()).isEqualTo("tenant-1");
        assertThat(actual.stageCount()).isEqualTo(2);
        assertThat(actual.taskCount()).isEqualTo(5);
        assertThat(actual.cpuTimeMs()).isEqualTo(10L);
        assertThat(actual.wallTimeMs()).isEqualTo(20L);
        assertThat(actual.peakMemoryBytes()).isEqualTo(30L);
        assertThat(actual.spilledBytes()).isEqualTo(40L);
        assertThat(actual.inputRows()).isEqualTo(50L);
        assertThat(actual.outputRows()).isEqualTo(60L);
        assertThat(actual.inputBytes()).isEqualTo(70L);
        assertThat(actual.outputBytes()).isEqualTo(80L);
        assertThat(actual.shuffleReadBytes()).isEqualTo(90L);
        assertThat(actual.shuffleWriteBytes()).isEqualTo(100L);
        assertThat(actual.completedAt()).isEqualTo(completedAt);
        assertThat(actual.success()).isTrue();
    }

    @Test
    void reSavingSameQueryOverwritesInsteadOfFailingOnPrimaryKey() throws Exception {
        DataSource source = MigratedPostgres.create();
        var store = new JdbcQueryAuditStore(source);
        Instant completedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        store.save(record("q-retry", completedAt));
        store.save(record("q-retry", completedAt));

        assertThat(store.findById("q-retry")).isPresent();
    }

    @Test
    void unknownQueryIsEmpty() throws Exception {
        DataSource source = MigratedPostgres.create();
        assertThat(new JdbcQueryAuditStore(source).findById("q-absent")).isEmpty();
    }

    @Test
    void retentionDeletesOnlyRowsOlderThanTheWindow() throws Exception {
        DataSource source = MigratedPostgres.create();
        var store = new JdbcQueryAuditStore(source);
        Instant now = Instant.now();
        store.save(record("q-old", now.minus(40, ChronoUnit.DAYS)));
        store.save(record("q-new", now));

        long deleted = store.deleteOlderThan(30);

        assertThat(deleted).isEqualTo(1L);
        assertThat(store.findById("q-old")).isEmpty();
        assertThat(store.findById("q-new")).isPresent();
    }

    @Test
    void retentionRejectsNonPositiveWindow() throws Exception {
        DataSource source = MigratedPostgres.create();
        var store = new JdbcQueryAuditStore(source);
        assertThatThrownBy(() -> store.deleteOlderThan(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.deleteOlderThan(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void successfulQueryRecordsAreDurableAcrossStoreInstances() throws Exception {
        DataSource source = MigratedPostgres.create();
        new JdbcQueryAuditStore(source).save(record("q-durable", Instant.now()));

        // A fresh instance stands in for a Coordinator restart: the in-memory
        // store would have lost this record entirely.
        assertThat(new JdbcQueryAuditStore(source).findById("q-durable")).isPresent();
    }

    @Test
    void nullCompletedAtIsPersistedAsUnknownTime() throws Exception {
        DataSource source = MigratedPostgres.create();
        var store = new JdbcQueryAuditStore(source);
        store.save(record("q-no-time", null));
        assertThat(store.findById("q-no-time").orElseThrow().completedAt()).isNull();
    }

    @Test
    void timestampsAreReadBackAsUtcInstants() throws Exception {
        DataSource source = MigratedPostgres.create();
        var store = new JdbcQueryAuditStore(source);
        Instant expected = Instant.parse("2026-01-02T03:04:05Z");
        store.save(record("q-instant", expected));

        try (var connection = source.getConnection();
             var statement = connection.prepareStatement(
                     "SELECT completed_at FROM query_audit_records WHERE query_id = ?")) {
            statement.setString(1, "q-instant");
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                Timestamp stored = rows.getTimestamp(1);
                assertThat(stored).isNotNull();
            }
        }
        assertThat(store.findById("q-instant").orElseThrow().completedAt()).isEqualTo(expected);
    }
}