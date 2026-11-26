package org.pangrova.testing.integration.e2e.index;

import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;

import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for index generation rollback and DROP semantics.
 *
 * <p>Proves that index versioning with generation fencing (task 5.2)
 * supports rollback by keeping N-1 generations, and that DROP INDEX
 * correctly marks all generations as DROPPED for GC cleanup.
 *
 * <p>DEDUP: {@link GcGenerationFencingE2ETest} covers REBUILD lifecycle
 * and GC safety. {@link GenerationIsolationE2ETest} covers data-change
 * correctness. This test focuses on rollback retention and DROP semantics.
 */
@DisplayName("Index Generation Rollback E2E")
class IndexGenerationRollbackE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @BeforeEach
    void createTestTable(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_rollback_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, value BIGINT, status VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    // ── Scenario 1: GC keeps N-1 for rollback ──

    @Nested
    @DisplayName("Scenario 1 — GC keeps N-1 for rollback")
    class GcKeepsNMinusOneForRollback {

        @Test
        @DisplayName("After REBUILD (gen 2), gen 1 index files still exist for rollback")
        void previousGenerationRetainedAfterRebuild() {
            // Seed data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (1, 100, 'ACTIVE'), (2, 200, 'ACTIVE')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (3, 300, 'INACTIVE'), (4, 400, 'INACTIVE')");

            // Create index (gen 1)
            sqlRunner.executeDdl("CREATE INDEX idx_rb ON " + fqn()
                    + "(status) USING BLOOM_FILTER");
            IndexReadiness.awaitReady(sqlRunner, fqn(), "idx_rb");

            // Verify gen 1 exists and works
            List<TypedRow> beforeRebuild = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'ACTIVE'");
            assertThat(beforeRebuild).hasSize(2);

            // Trigger rebuild → gen 2
            sqlRunner.executeDdl("ALTER INDEX idx_rb ON " + fqn() + " REBUILD");

            // Index should still be functional (gen 2 active)
            List<TypedRow> afterRebuild = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'ACTIVE'");
            assertThat(afterRebuild).hasSize(2);

            // Verify via metadata store: gen 1 files still exist (N-1 kept for rollback)
            // GC policy: only delete generation N-2 and older, keep N-1
            await("both physical index generations")
                    .atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertThat(countIndexGenerations(tableName, "idx_rb"))
                            .as("Both gen 1 (rollback) and gen 2 (active) should exist")
                            .isGreaterThanOrEqualTo(2));

            // Verify SHOW INDEXES still lists the index as active
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r ->
                    "idx_rb".equals(r.getRaw("index_name").toString()));
        }

        @Test
        @DisplayName("Query correctness preserved across rebuild with rollback retention")
        void queryCorrectnessAcrossRebuildWithRollback() {
            // Seed data across multiple inserts
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (1, 10, 'X'), (2, 20, 'Y')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (3, 30, 'X'), (4, 40, 'Z')");

            // Create index (gen 1)
            sqlRunner.executeDdl("CREATE INDEX idx_rb2 ON " + fqn()
                    + "(status) USING BLOOM_FILTER");
            IndexReadiness.awaitReady(sqlRunner, fqn(), "idx_rb2");

            // Verify gen 1 query
            List<TypedRow> gen1Result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'X'");
            assertThat(gen1Result).hasSize(2);

            // Insert more data before rebuild
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (5, 50, 'X')");

            // Rebuild → gen 2
            sqlRunner.executeDdl("ALTER INDEX idx_rb2 ON " + fqn() + " REBUILD");

            // Gen 2 should cover all data including post-gen1 inserts
            List<TypedRow> gen2Result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'X'");
            assertThat(gen2Result).hasSize(3);

            // Full table scan should return all 5 rows
            List<TypedRow> allRows = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(allRows).hasSize(5);
        }
    }

    // ── Scenario 2: DROP INDEX marks all generations ──

    @Nested
    @DisplayName("Scenario 2 — DROP INDEX marks all generations DROPPED")
    class DropIndexMarksAllGenerations {

        @Test
        @DisplayName("DROP INDEX after CREATE marks all generations DROPPED and GC cleans up")
        void dropIndexMarksAllGenerationsDropped() {
            // Seed data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (1, 100, 'A'), (2, 200, 'B')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (3, 300, 'A'), (4, 400, 'C')");

            // Create index (gen 1)
            sqlRunner.executeDdl("CREATE INDEX idx_drop ON " + fqn()
                    + "(status) USING BLOOM_FILTER");

            // Verify index exists
            List<TypedRow> indexesBefore = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexesBefore).anyMatch(r ->
                    "idx_drop".equals(r.getRaw("index_name").toString()));

            // DROP INDEX — should mark all generations as DROPPED
            sqlRunner.executeDdl("DROP INDEX idx_drop ON " + fqn());

            // SHOW INDEXES filters out DROPPED — index should not appear
            List<TypedRow> indexesAfter = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            boolean hasActiveIndex = indexesAfter.stream()
                    .anyMatch(r -> "idx_drop".equals(r.getRaw("index_name").toString()));
            assertThat(hasActiveIndex)
                    .as("Dropped index should not appear in SHOW INDEXES")
                    .isFalse();

            // Verify all generations are marked DROPPED in metadata store
            int activeDescriptors = countActiveIndexDescriptors(tableName, "idx_drop");
            assertThat(activeDescriptors)
                    .as("No active index descriptor should remain after DROP INDEX")
                    .isZero();

            // Query should still work correctly without index
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'A'");
            assertThat(rows).hasSize(2);
        }

        @Test
        @DisplayName("DROP INDEX after REBUILD marks both generations DROPPED")
        void dropAfterRebuildMarksAllGenerationsDropped() {
            // Seed data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (1, 10, 'P'), (2, 20, 'Q')");

            // Create index (gen 1)
            sqlRunner.executeDdl("CREATE INDEX idx_drop2 ON " + fqn()
                    + "(status) USING BLOOM_FILTER");
            IndexReadiness.awaitReady(sqlRunner, fqn(), "idx_drop2");
            JdbcIndexMetadataStore store = new JdbcIndexMetadataStore(stateStoreDataSource(), TableWriteAuthorityFixture.create()::requireCurrent);
            IndexDescriptor initial = requireIndex(store, "idx_drop2");
            long initialSnapshot = Long.parseLong(initial.buildSnapshotId());
            SnapshotPinProvider pins = snapshotPins();
            assertThat(pins.getSnapshotPinsForIndex(CATALOG, SCHEMA, tableName, initial.indexId()))
                    .extracting(org.pangrova.connector.api.index.SnapshotPinReference::snapshotToken)
                    .contains(Long.toString(initialSnapshot));
            List<IndexFileMapping> initialFiles = store.getIndexFileMappings(initial.indexId());
            assertThat(initialFiles).isNotEmpty().allSatisfy(mapping -> {
                assertThat(mapping.generation()).isEqualTo(initial.generation());
                assertPhysicalIndexFile(mapping);
            });

            // Force a distinct Iceberg snapshot so the rebuild must own another tag.
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (3, 30, 'R')");

            // Rebuild → gen 2
            sqlRunner.executeDdl("ALTER INDEX idx_drop2 ON " + fqn() + " REBUILD");
            await("rebuilt scalar generation and physical files")
                    .atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> {
                        IndexDescriptor rebuilt = requireIndex(store, "idx_drop2");
                        assertThat(rebuilt.state()).isEqualTo(IndexState.READY);
                        assertThat(rebuilt.generation()).isEqualTo(initial.generation() + 1);
                        assertThat(rebuilt.buildSnapshotId()).isNotEqualTo(initial.buildSnapshotId());
                        long rebuiltSnapshot = Long.parseLong(rebuilt.buildSnapshotId());
                        assertThat(pins.getSnapshotPinsForIndex(
                                CATALOG, SCHEMA, tableName, initial.indexId()))
                                .extracting(org.pangrova.connector.api.index.SnapshotPinReference::snapshotToken)
                                .containsExactlyInAnyOrder(
                                        Long.toString(initialSnapshot), Long.toString(rebuiltSnapshot));
                        List<IndexFileMapping> files = store.getIndexFileMappings(initial.indexId());
                        assertThat(files).extracting(IndexFileMapping::generation)
                                .contains(initial.generation(), rebuilt.generation());
                        files.forEach(mapping -> assertPhysicalIndexFile(mapping));
                    });

            // Verify index still active after rebuild
            List<TypedRow> indexesMid = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexesMid).anyMatch(r ->
                    "idx_drop2".equals(r.getRaw("index_name").toString()));

            // DROP INDEX — should mark ALL generations (gen 1 + gen 2) as DROPPED
            sqlRunner.executeDdl("DROP INDEX idx_drop2 ON " + fqn());
            assertThat(store.findIndex(initial.indexId()).orElseThrow().state())
                    .isEqualTo(IndexState.DROPPED);
            // DROP retires query use immediately. The default grace period keeps
            // both physical generations and Iceberg refs until the GC cycle.
            assertThat(pins.getSnapshotPinsForIndex(CATALOG, SCHEMA, tableName, initial.indexId()))
                    .hasSize(2);
            store.getIndexFileMappings(initial.indexId()).forEach(mapping -> assertPhysicalIndexFile(mapping));

            // Verify no active index remains
            List<TypedRow> indexesAfter = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            boolean hasActiveIndex = indexesAfter.stream()
                    .anyMatch(r -> "idx_drop2".equals(r.getRaw("index_name").toString()));
            assertThat(hasActiveIndex)
                    .as("All generations should be DROPPED after DROP INDEX")
                    .isFalse();

            // Verify via metadata: no non-DROPPED generations remain
            int activeDescriptors = countActiveIndexDescriptors(tableName, "idx_drop2");
            assertThat(activeDescriptors)
                    .as("No active index descriptor should remain after DROP INDEX")
                    .isZero();

            // Query correctness without index
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'P'");
            assertThat(rows).hasSize(1);
        }

        @Test
        @DisplayName("DROP retires the index after a real file build and data remains queryable")
        void dropRetiresIndexAfterRealFileBuild() {
            // Seed substantial data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (1, 100, 'ACTIVE'), (2, 200, 'ACTIVE')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (3, 300, 'INACTIVE'), (4, 400, 'INACTIVE')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (5, 500, 'ACTIVE'), (6, 600, 'PENDING')");

            // Verify a real physical build before checking the DROP state.
            sqlRunner.executeDdl("CREATE INDEX idx_stale ON " + fqn()
                    + "(status) USING BLOOM_FILTER");
            IndexReadiness.awaitReady(sqlRunner, fqn(), "idx_stale");
            assertThat(countIndexFileMappings(tableName, "idx_stale"))
                    .as("the index has persisted file mappings before DROP")
                    .isPositive();
            sqlRunner.executeDdl("DROP INDEX idx_stale ON " + fqn());

            // Physical mappings are retained until GC safely releases the
            // snapshot pin; DROP must retire the published descriptor now.
            assertThat(countActiveIndexDescriptors(tableName, "idx_stale"))
                    .as("the dropped index is no longer active")
                    .isZero();

            // All data should still be queryable
            List<TypedRow> allRows = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(allRows).hasSize(6);

            // Filtered queries should return correct results
            List<TypedRow> activeRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'ACTIVE'");
            assertThat(activeRows).hasSize(3);
        }
    }

    // ── Metadata store helpers ──

    /**
     * Counts distinct generations for a given index in the metadata store.
     */
    private int countIndexGenerations(String table, String indexName) {
        try (Connection conn = stateStoreDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COUNT(DISTINCT mappings.generation) FROM index_file_mappings mappings "
                             + "JOIN index_descriptors descriptors ON mappings.index_id = descriptors.index_id "
                             + "WHERE descriptors.tenant_id = ? AND descriptors.catalog_name = ? "
                             + "AND descriptors.schema_name = ? AND descriptors.table_name = ? "
                             + "AND descriptors.index_name = ?")) {
            bindIndexIdentity(ps, table, indexName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect persisted index generations", e);
        }
    }

    private IndexDescriptor requireIndex(JdbcIndexMetadataStore store, String indexName) {
        return store.findIndexesByTable(
                        authenticatedCatalogSession(CATALOG).getProperties().get("tenant_id"),
                        CATALOG, SCHEMA, tableName).stream()
                .filter(index -> index.indexName().equals(indexName))
                .findFirst().orElseThrow();
    }

    private SnapshotPinProvider snapshotPins() {
        return metadataBootstrap.catalogRegistry().getCatalog(CATALOG)
                .getMetadata(authenticatedCatalogSession(CATALOG))
                .getIndexSupport().orElseThrow()
                .getSnapshotPinProvider().orElseThrow();
    }

    private void assertPhysicalIndexFile(IndexFileMapping mapping) {
        Path path = Path.of(mapping.indexFilePath());
        assertThat(Files.isRegularFile(path)).as("persisted scalar index file: %s", path).isTrue();
        try {
            assertThat(Files.size(path)).isEqualTo(mapping.indexSizeBytes());
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("Cannot read persisted scalar index file " + path, failure);
        }
    }

    /**
     * Counts non-DROPPED descriptors for the exact tenant and table.
     */
    private int countActiveIndexDescriptors(String table, String indexName) {
        try (Connection conn = stateStoreDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COUNT(*) FROM index_descriptors "
                             + "WHERE tenant_id = ? AND catalog_name = ? AND schema_name = ? "
                             + "AND table_name = ? AND index_name = ? AND state != 'DROPPED'")) {
            bindIndexIdentity(ps, table, indexName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect persisted index state", e);
        }
    }

    /**
     * Counts index file mappings for a given index (detects stale files).
     */
    private int countIndexFileMappings(String table, String indexName) {
        try (Connection conn = stateStoreDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COUNT(*) FROM index_file_mappings mappings "
                             + "JOIN index_descriptors descriptors ON mappings.index_id = descriptors.index_id "
                             + "WHERE descriptors.tenant_id = ? AND descriptors.catalog_name = ? "
                             + "AND descriptors.schema_name = ? AND descriptors.table_name = ? "
                             + "AND descriptors.index_name = ?")) {
            bindIndexIdentity(ps, table, indexName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect persisted index file mappings", e);
        }
    }

    private void bindIndexIdentity(PreparedStatement statement, String table, String indexName)
            throws SQLException {
        statement.setString(1, authenticatedCatalogSession(CATALOG).getProperties().get("tenant_id"));
        statement.setString(2, CATALOG);
        statement.setString(3, SCHEMA);
        statement.setString(4, table);
        statement.setString(5, indexName);
    }
}
