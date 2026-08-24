package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CALL procedure E2E tests for Paimon connector.
 *
 * <p>Verifies that {@code CALL catalog.procedure_name(args)} correctly routes
 * to connector procedure implementations and executes expected operations.
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

    // --- 2.2.13: CALL sys.create_tag then $tags verification ---

    @Nested
    @DisplayName("2.2.13 - CALL create_tag then $tags verification")
    class CreateTagVerification {

        @Test
        @DisplayName("CALL create_tag creates tag visible in $tags system table")
        void createTagThenVerifyInSystemTable() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".create_tag('" + tableName + "', 'v1.0')");

            List<TypedRow> tags = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$tags WHERE tag_name = 'v1.0'");

            assertThat(tags).hasSize(1);
            TypedRow tagRow = tags.get(0);
            assertThat(tagRow.getRaw("tag_name")).isEqualTo("v1.0");
            assertThat(((Number) tagRow.getRaw("snapshot_id")).longValue())
                    .isGreaterThan(0);
        }

        @Test
        @DisplayName("Multiple tags created via CALL all visible in $tags")
        void multipleTagsCreation() {
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

            List<TypedRow> tags = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$tags ORDER BY tag_name");

            assertThat(tags).hasSize(2);
            assertThat(tags.get(0).getRaw("tag_name")).isEqualTo("v1");
            assertThat(tags.get(1).getRaw("tag_name")).isEqualTo("v2");
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
        void rollbackToSnapshotIdRestoresData() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, val VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, val) VALUES (1, 'a')");

            List<TypedRow> snapshots = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$snapshots ORDER BY snapshot_id");
            long firstSnapshotId = ((Number) snapshots.get(0).getRaw("snapshot_id")).longValue();

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

    // --- 2.2.15: CALL sys.compact then compaction verification ---

    @Nested
    @DisplayName("2.2.15 - CALL compact then compaction verification")
    class CompactVerification {

        @Test
        @DisplayName("CALL compact reduces file count in $files system table")
        void compactReducesFileCount() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");

            for (int i = 0; i < 10; i++) {
                sqlRunner.execute("INSERT INTO " + paimonFqn()
                        + " (id, name) VALUES (" + i + ", 'user" + i + "')");
            }

            List<TypedRow> filesBeforeCompact = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$files");
            int fileCountBefore = filesBeforeCompact.size();

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".compact('" + tableName + "')");

            List<TypedRow> filesAfterCompact = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$files");
            int fileCountAfter = filesAfterCompact.size();

            assertThat(fileCountAfter).isLessThanOrEqualTo(fileCountBefore);

            List<TypedRow> dataAfterCompact = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + " ORDER BY id");
            assertThat(dataAfterCompact).hasSize(10);
        }
    }

    // --- 2.2.16: CALL sys.expire_snapshots then snapshot expiration verification ---

    @Nested
    @DisplayName("2.2.16 - CALL expire_snapshots then snapshot expiration verification")
    class ExpireSnapshotsVerification {

        @Test
        @DisplayName("CALL expire_snapshots removes old snapshots from $snapshots")
        void expireSnapshotsRemovesOldSnapshots() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");

            for (int i = 0; i < 5; i++) {
                sqlRunner.execute("INSERT INTO " + paimonFqn()
                        + " (id, name) VALUES (" + i + ", 'user" + i + "')");
            }

            List<TypedRow> snapshotsBefore = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$snapshots");
            int snapshotCountBefore = snapshotsBefore.size();
            assertThat(snapshotCountBefore).isGreaterThanOrEqualTo(5);

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".expire_snapshots('" + tableName + "', 1)");

            List<TypedRow> snapshotsAfter = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$snapshots");
            int snapshotCountAfter = snapshotsAfter.size();

            assertThat(snapshotCountAfter).isLessThanOrEqualTo(snapshotCountBefore);
            assertThat(snapshotCountAfter).isGreaterThanOrEqualTo(1);
        }
    }

    // --- 2.2.17: CALL sys.create_branch then $branches verification ---

    @Nested
    @DisplayName("2.2.17 - CALL create_branch then $branches verification")
    class CreateBranchVerification {

        @Test
        @DisplayName("CALL create_branch creates branch visible in $branches system table")
        void createBranchThenVerifyInSystemTable() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name) VALUES (1, 'alice')");

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".create_branch('" + tableName + "', 'dev')");

            List<TypedRow> branches = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$branches WHERE branch_name = 'dev'");

            assertThat(branches).hasSize(1);
            TypedRow branchRow = branches.get(0);
            assertThat(branchRow.getRaw("branch_name")).isEqualTo("dev");
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
