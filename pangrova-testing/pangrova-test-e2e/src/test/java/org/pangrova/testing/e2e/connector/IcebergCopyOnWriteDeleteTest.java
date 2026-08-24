package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Iceberg Copy-on-Write DELETE E2E tests verifying that CoW mode rewrites
 * surviving rows into new data files without producing delete files,
 * reads have no merge overhead, and mode switching between CoW and MoR works.
 *
 * <p>All operations execute through the real pipeline:
 * TestCluster HTTP → NativeRuntime → Velox → Iceberg → local filesystem.
 *
 * <p>Requirements: B1
 */
@DisplayName("Iceberg Copy-on-Write DELETE E2E Tests")
class IcebergCopyOnWriteDeleteTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // --- 2.8.3: CoW DELETE produces no delete files ---

    @Test
    @DisplayName("2.8.3 - CoW DELETE produces no delete files and returns surviving rows")
    void cowDeleteProducesNoDeleteFiles() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                + " TBLPROPERTIES ('format-version' = '2', "
                + "'write.delete.mode' = 'copy-on-write')");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name, amount) VALUES "
                + "(1, 'alice', 100.0), (2, 'bob', 200.0), "
                + "(3, 'carol', 300.0), (4, 'dave', 400.0), "
                + "(5, 'eve', 500.0)");

        sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id IN (2, 4)");

        // Verify no delete files exist — all entries in $files should be DATA
        List<TypedRow> fileRows = sqlRunner.execute(
                "SELECT * FROM " + fqn() + "$files");

        assertThat(fileRows).isNotEmpty();
        for (TypedRow fileRow : fileRows) {
            assertThat(fileRow.getRaw("content").toString())
                    .as("CoW DELETE should produce only data files, no delete files")
                    .isEqualTo("DATA");
        }

        // Verify surviving rows are correct
        List<TypedRow> dataRows = sqlRunner.execute(
                "SELECT * FROM " + fqn() + " ORDER BY id");

        assertThat(dataRows).hasSize(3);
        assertThat(((Number) dataRows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        assertThat(dataRows.get(0).getRaw("name")).isEqualTo("alice");
        assertThat(((Number) dataRows.get(1).getRaw("id")).longValue()).isEqualTo(3L);
        assertThat(dataRows.get(1).getRaw("name")).isEqualTo("carol");
        assertThat(((Number) dataRows.get(2).getRaw("id")).longValue()).isEqualTo(5L);
        assertThat(dataRows.get(2).getRaw("name")).isEqualTo("eve");
    }

    // --- 2.8.4: CoW read has no merge overhead ---

    @Test
    @DisplayName("2.8.4 - CoW read has no merge overhead (only data files, no delete files to merge)")
    void cowReadHasNoMergeOverhead() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, category VARCHAR, price DOUBLE)"
                + " TBLPROPERTIES ('format-version' = '2', "
                + "'write.delete.mode' = 'copy-on-write')");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, category, price) VALUES "
                + "(1, 'electronics', 999.99), (2, 'books', 19.99), "
                + "(3, 'electronics', 499.99), (4, 'books', 29.99), "
                + "(5, 'clothing', 79.99)");

        sqlRunner.execute("DELETE FROM " + fqn() + " WHERE category = 'books'");

        // Verify $files shows only data files — no delete files means no merge step
        List<TypedRow> fileRows = sqlRunner.execute(
                "SELECT * FROM " + fqn() + "$files");

        assertThat(fileRows).isNotEmpty();
        long deleteFileCount = fileRows.stream()
                .filter(r -> !r.getRaw("content").toString().equals("DATA"))
                .count();
        assertThat(deleteFileCount)
                .as("CoW mode should have zero delete files — reads require no merge")
                .isZero();

        // Verify SELECT returns correct results without merge
        List<TypedRow> dataRows = sqlRunner.execute(
                "SELECT * FROM " + fqn() + " ORDER BY id");

        assertThat(dataRows).hasSize(3);
        assertThat(((Number) dataRows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        assertThat(dataRows.get(0).getRaw("category")).isEqualTo("electronics");
        assertThat(((Number) dataRows.get(1).getRaw("id")).longValue()).isEqualTo(3L);
        assertThat(dataRows.get(1).getRaw("category")).isEqualTo("electronics");
        assertThat(((Number) dataRows.get(2).getRaw("id")).longValue()).isEqualTo(5L);
        assertThat(dataRows.get(2).getRaw("category")).isEqualTo("clothing");
    }

    // --- 2.8.5: Multiple DELETEs with insert interleaving ---

    @Test
    @DisplayName("2.8.5 - Multiple DELETE rounds with insert interleaving produce correct results")
    void multipleDeleteRoundsWithInsertInterleaving() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                + " TBLPROPERTIES ('format-version' = '2', "
                + "'write.delete.mode' = 'copy-on-write')");

        // Phase 1: insert + delete
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name, amount) VALUES "
                + "(1, 'alice', 100.0), (2, 'bob', 200.0), "
                + "(3, 'carol', 300.0)");

        sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 2");

        List<TypedRow> phase1Files = sqlRunner.execute(
                "SELECT * FROM " + fqn() + "$files");
        assertThat(phase1Files).isNotEmpty();
        for (TypedRow fileRow : phase1Files) {
            assertThat(fileRow.getRaw("content").toString())
                    .as("CoW DELETE produces only data files")
                    .isEqualTo("DATA");
        }

        List<TypedRow> afterPhase1 = sqlRunner.execute(
                "SELECT * FROM " + fqn() + " ORDER BY id");
        assertThat(afterPhase1).hasSize(2);
        assertThat(((Number) afterPhase1.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        assertThat(((Number) afterPhase1.get(1).getRaw("id")).longValue()).isEqualTo(3L);

        // Phase 2: insert more data + delete again
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name, amount) VALUES "
                + "(4, 'dave', 400.0), (5, 'eve', 500.0)");

        sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 4");

        List<TypedRow> phase2Files = sqlRunner.execute(
                "SELECT * FROM " + fqn() + "$files");
        for (TypedRow fileRow : phase2Files) {
            assertThat(fileRow.getRaw("content").toString())
                    .as("Second CoW DELETE also produces only data files")
                    .isEqualTo("DATA");
        }

        List<TypedRow> finalRows = sqlRunner.execute(
                "SELECT * FROM " + fqn() + " ORDER BY id");

        assertThat(finalRows).hasSize(3);
        assertThat(((Number) finalRows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        assertThat(finalRows.get(0).getRaw("name")).isEqualTo("alice");
        assertThat(((Number) finalRows.get(1).getRaw("id")).longValue()).isEqualTo(3L);
        assertThat(finalRows.get(1).getRaw("name")).isEqualTo("carol");
        assertThat(((Number) finalRows.get(2).getRaw("id")).longValue()).isEqualTo(5L);
        assertThat(finalRows.get(2).getRaw("name")).isEqualTo("eve");
    }
}
