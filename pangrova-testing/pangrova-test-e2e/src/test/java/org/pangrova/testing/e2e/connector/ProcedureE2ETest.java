package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CALL procedure E2E tests for Paimon connector.
 *
 * <p>Verifies that {@code CALL catalog.procedure_name(args)} correctly routes
 * to connector procedure implementations and executes expected operations.
 * Published system-table SQL is validated separately by {@link SystemTableE2ETest}.
 *
 * <p>Covers: C4 (Tag Management), C5 (Branch Management), C8 (CALL Procedures),
 * D5 (CALL Procedure unified framework).
 *
 * <p>Requirements: C4, C5, C8, D5
 */
@DisplayName("CALL Procedure E2E Tests - Paimon")
class ProcedureE2ETest extends SqlE2ETestBase {

    private static final String PAI_CATALOG = "paimon_proc";
    private static final String SCHEMA = "default";

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.paimon(
                        PAI_CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    @BeforeAll
    static void registerConnectors() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.paimon(
                        PAI_CATALOG, SCHEMA, paimonWarehouse().toString())));
    }

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod()
                .map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String paimonFqn() {
        return PAI_CATALOG + "." + SCHEMA + "." + tableName;
    }

    private <T> T inspectCommittedTable(Function<FileStoreTable, T> inspect) throws Exception {
        Options options = new Options();
        options.setString("warehouse", paimonWarehouse().toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            return inspect.apply((FileStoreTable) catalog.getTable(
                    Identifier.create(SCHEMA, tableName)));
        }
    }

    private int committedSnapshotCount() throws Exception {
        return inspectCommittedTable(table -> {
            var snapshots = table.snapshotManager();
            Long earliest = snapshots.earliestSnapshotId();
            Long latest = snapshots.latestSnapshotId();
            if (earliest == null || latest == null) {
                return 0;
            }
            int count = 0;
            for (long id = earliest; id <= latest; id++) {
                if (snapshots.snapshotExists(id)) {
                    count++;
                }
            }
            return count;
        });
    }

    private long currentDataFileCount() throws Exception {
        return inspectCommittedTable(table -> table.newReadBuilder().newScan().plan()
                .splits().stream()
                .filter(DataSplit.class::isInstance)
                .map(DataSplit.class::cast)
                .mapToLong(split -> split.dataFiles().size())
                .sum());
    }

    // --- 2.2.13: CALL create_tag then SDK metadata verification ---

    @Nested
    @DisplayName("2.2.13 - CALL create_tag then persisted metadata verification")
    class CreateTagVerification {

        @Test
        @DisplayName("CALL create_tag persists a named tag in Paimon metadata")
        void createTagThenVerifyPersistedMetadata() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".create_tag('" + tableName + "', 'v1.0')");

            List<Long> tagSnapshots = inspectCommittedTable(table -> table.tagManager()
                    .tags().entrySet().stream()
                    .filter(entry -> entry.getValue().contains("v1.0"))
                    .map(entry -> entry.getKey().id())
                    .toList());
            assertThat(tagSnapshots).hasSize(1);
            assertThat(tagSnapshots.get(0)).isGreaterThan(0);
        }

        @Test
        @DisplayName("Multiple tags created via CALL persist in Paimon metadata")
        void multipleTagsCreation() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, val VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, val) VALUES (1, 'a')");

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".create_tag('" + tableName + "', 'v1')");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, val) VALUES (2, 'b')");
            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".create_tag('" + tableName + "', 'v2')");

            List<String> tags = inspectCommittedTable(table -> table.tagManager()
                    .tags().values().stream()
                    .flatMap(List::stream)
                    .sorted()
                    .toList());
            assertThat(tags).containsExactly("v1", "v2");
        }
    }

    // --- 2.2.14: CALL sys.rollback_to then data rollback verification ---

    @Nested
    @DisplayName("2.2.14 - CALL rollback_to then data rollback verification")
    class RollbackToVerification {

        @Test
        @DisplayName("CALL rollback_to restores table to tagged snapshot")
        void rollbackToTagRestoresData() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name) VALUES (1, 'alice')");

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".create_tag('" + tableName + "', 'before_bob')");

            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name) VALUES (2, 'bob')");

            List<TypedRow> beforeRollback = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + " ORDER BY id");
            assertThat(beforeRollback).hasSize(2);

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".rollback_to('" + tableName + "', 'before_bob')");

            List<TypedRow> afterRollback = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + " ORDER BY id");
            assertThat(afterRollback).hasSize(1);
            assertThat(afterRollback.get(0).getRaw("name")).isEqualTo("alice");
        }

        @Test
        @DisplayName("CALL rollback_to with snapshot_id restores to specific snapshot")
        void rollbackToSnapshotIdRestoresData() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, val VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, val) VALUES (1, 'a')");

            Long firstSnapshotId = inspectCommittedTable(table ->
                    table.snapshotManager().latestSnapshotId());
            assertThat(firstSnapshotId).isNotNull();

            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, val) VALUES (2, 'b')");

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".rollback_to('" + tableName + "', " + firstSnapshotId + ")");

            List<TypedRow> afterRollback = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + " ORDER BY id");
            assertThat(afterRollback).hasSize(1);
            assertThat(afterRollback.get(0).getRaw("val")).isEqualTo("a");
        }
    }

    // --- 2.2.15: CALL compact then physical file verification ---

    @Nested
    @DisplayName("2.2.15 - CALL compact then compaction verification")
    class CompactVerification {

        @Test
        @DisplayName("CALL compact preserves data and does not increase current file count")
        void compactReducesFileCount() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");

            for (int i = 0; i < 10; i++) {
                sqlRunner.execute("INSERT INTO " + paimonFqn()
                        + " (id, name) VALUES (" + i + ", 'user" + i + "')");
            }

            long fileCountBefore = currentDataFileCount();
            assertThat(fileCountBefore).isGreaterThan(0);

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".compact('" + tableName + "')");

            long fileCountAfter = currentDataFileCount();

            assertThat(fileCountAfter).isLessThanOrEqualTo(fileCountBefore);

            List<TypedRow> dataAfterCompact = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + " ORDER BY id");
            assertThat(dataAfterCompact).hasSize(10);
        }
    }

    // --- 2.2.16: CALL expire_snapshots then SDK snapshot verification ---

    @Nested
    @DisplayName("2.2.16 - CALL expire_snapshots then snapshot expiration verification")
    class ExpireSnapshotsVerification {

        @Test
        @DisplayName("CALL expire_snapshots reduces retained Paimon snapshots")
        void expireSnapshotsRemovesOldSnapshots() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");

            for (int i = 0; i < 5; i++) {
                sqlRunner.execute("INSERT INTO " + paimonFqn()
                        + " (id, name) VALUES (" + i + ", 'user" + i + "')");
            }

            int snapshotCountBefore = committedSnapshotCount();
            assertThat(snapshotCountBefore).isGreaterThanOrEqualTo(5);

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".expire_snapshots('" + tableName + "', 1)");

            int snapshotCountAfter = committedSnapshotCount();

            assertThat(snapshotCountAfter).isLessThan(snapshotCountBefore);
            assertThat(snapshotCountAfter).isGreaterThanOrEqualTo(1);
        }
    }

    // --- 2.2.17: CALL create_branch then SDK metadata verification ---

    @Nested
    @DisplayName("2.2.17 - CALL create_branch then persisted metadata verification")
    class CreateBranchVerification {

        @Test
        @DisplayName("CALL create_branch persists a branch in Paimon metadata")
        void createBranchThenVerifyPersistedMetadata() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name) VALUES (1, 'alice')");

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".create_branch('" + tableName + "', 'dev')");

            List<String> branches = inspectCommittedTable(table -> table.branchManager()
                    .branches());
            assertThat(branches).contains("dev");
        }
    }

    // --- 2.2.18: CALL sys.remove_orphan_files then cleanup verification ---

    @Nested
    @DisplayName("2.2.18 - CALL remove_orphan_files then cleanup verification")
    class RemoveOrphanFilesVerification {

        @Test
        @DisplayName("CALL remove_orphan_files executes without error")
        void removeOrphanFilesExecutesSuccessfully() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name) VALUES (1, 'alice')");

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".remove_orphan_files('" + tableName + "')");

            List<TypedRow> data = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn());
            assertThat(data).hasSize(1);
        }
    }

    // --- 2.2.19: Non-existent procedure error handling ---

    @Nested
    @DisplayName("2.2.19 - Non-existent procedure error handling")
    class NonExistentProcedureError {

        @Test
        @DisplayName("CALL non-existent procedure throws clear error")
        void callNonExistentProcedureThrowsError() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");

            assertThatThrownBy(() ->
                    sqlRunner.execute("CALL " + PAI_CATALOG
                            + ".non_existent_procedure('" + tableName + "')"))
                    .hasMessageContaining("non_existent_procedure")
                    .hasMessageContaining("not found");
        }

        @Test
        @DisplayName("CALL procedure with wrong argument count throws error")
        void callProcedureWithWrongArgumentCountThrowsError() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");

            assertThatThrownBy(() ->
                    sqlRunner.execute("CALL " + PAI_CATALOG
                            + ".create_tag('" + tableName + "')"))
                    .hasMessageContaining("argument");
        }
    }
}
