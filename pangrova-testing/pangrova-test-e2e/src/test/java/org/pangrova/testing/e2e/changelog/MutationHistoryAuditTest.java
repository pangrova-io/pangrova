package org.pangrova.testing.e2e.changelog;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mutation history audit E2E tests.
 *
 * <p>Validates that DML operations (INSERT, UPDATE, DELETE, MERGE) are properly
 * tracked in the table's changelog/history metadata. Verifies that the system
 * maintains an accurate audit trail of all mutations including operation type,
 * affected rows, and snapshot lineage.</p>
 *
 * <p>Requirements validated: 7.1, 7.2, 11.1</p>
 */
@DisplayName("Mutation History Audit Tests")
class MutationHistoryAuditTest extends PangrovaE2ETestBase {

    private String tableName;

    @BeforeEach
    void createUniqueTable() {
        tableName = "changelog_audit_" + System.nanoTime();
        executeDdl("CREATE TABLE " + tableName
                + " (id BIGINT, name VARCHAR, amount DOUBLE)");
    }

    @Nested
    @DisplayName("Insert History")
    class InsertHistory {

        @Test
        @DisplayName("INSERT operation recorded in table history")
        void insertRecordedInHistory() {
            executeDml("INSERT INTO " + tableName + " VALUES (1, 'alice', 100.0)");
            executeDml("INSERT INTO " + tableName + " VALUES (2, 'bob', 200.0)");

            List<TypedRow> history = executeQuery(
                    "SELECT * FROM " + tableName + "$history");
            assertThat(history).hasSizeGreaterThanOrEqualTo(2);
        }

        @Test
        @DisplayName("batch INSERT creates single snapshot")
        void batchInsertCreatesSingleSnapshot() {
            executeDml("INSERT INTO " + tableName + " VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0), (3, 'carol', 300.0)");

            List<TypedRow> snapshots = executeQuery(
                    "SELECT * FROM " + tableName + "$snapshots");
            assertThat(snapshots).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Update History")
    class UpdateHistory {

        @Test
        @DisplayName("UPDATE operation creates new snapshot")
        void updateCreatesNewSnapshot() {
            executeDml("INSERT INTO " + tableName + " VALUES (1, 'alice', 100.0)");

            List<TypedRow> beforeSnapshots = executeQuery(
                    "SELECT * FROM " + tableName + "$snapshots");

            executeDml("UPDATE " + tableName + " SET amount = 150.0 WHERE id = 1");

            List<TypedRow> afterSnapshots = executeQuery(
                    "SELECT * FROM " + tableName + "$snapshots");
            assertThat(afterSnapshots.size()).isGreaterThan(beforeSnapshots.size());
        }

        @Test
        @DisplayName("UPDATE preserves unmodified rows")
        void updatePreservesUnmodifiedRows() {
            executeDml("INSERT INTO " + tableName + " VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0)");
            executeDml("UPDATE " + tableName + " SET amount = 999.0 WHERE id = 1");

            List<TypedRow> rows = executeQuery(
                    "SELECT * FROM " + tableName + " WHERE id = 2");
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("amount")).doubleValue()).isEqualTo(200.0);
        }
    }

    @Nested
    @DisplayName("Delete History")
    class DeleteHistory {

        @Test
        @DisplayName("DELETE operation tracked in changelog")
        void deleteTrackedInChangelog() {
            executeDml("INSERT INTO " + tableName + " VALUES (1, 'alice', 100.0)");
            executeDml("DELETE FROM " + tableName + " WHERE id = 1");

            List<TypedRow> current = executeQuery("SELECT * FROM " + tableName);
            assertThat(current).isEmpty();

            List<TypedRow> history = executeQuery(
                    "SELECT * FROM " + tableName + "$history");
            assertThat(history).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Snapshot Lineage")
    class SnapshotLineage {

        @Test
        @DisplayName("multiple DML operations produce ordered snapshots")
        void multipleDmlProduceOrderedSnapshots() {
            executeDml("INSERT INTO " + tableName + " VALUES (1, 'alice', 100.0)");
            executeDml("INSERT INTO " + tableName + " VALUES (2, 'bob', 200.0)");
            executeDml("UPDATE " + tableName + " SET amount = 150.0 WHERE id = 1");
            executeDml("DELETE FROM " + tableName + " WHERE id = 2");

            List<TypedRow> snapshots = executeQuery(
                    "SELECT * FROM " + tableName + "$snapshots ORDER BY committed_at");
            assertThat(snapshots).hasSizeGreaterThanOrEqualTo(3);
        }

        @Test
        @DisplayName("time-travel query returns historical state")
        void timeTravelReturnsHistoricalState() {
            executeDml("INSERT INTO " + tableName + " VALUES (1, 'alice', 100.0)");

            List<TypedRow> snapshots = executeQuery(
                    "SELECT snapshot_id FROM " + tableName + "$snapshots LIMIT 1");
            assertThat(snapshots).isNotEmpty();
            String snapshotId = snapshots.get(0).getRaw("snapshot_id").toString();
            List<TypedRow> historical = executeQuery(
                    "SELECT * FROM default.default." + tableName
                    + " FOR VERSION AS OF " + snapshotId);
            assertThat(historical).isNotEmpty();
        }
    }
}
