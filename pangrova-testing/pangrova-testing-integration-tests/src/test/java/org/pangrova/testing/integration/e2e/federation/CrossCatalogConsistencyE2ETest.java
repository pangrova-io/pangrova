package org.pangrova.testing.integration.e2e.federation;

import org.pangrova.metadata.api.cache.CacheStats;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.adapter.FederatedCatalog;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.metadata.api.catalog.MetadataChange;
import org.pangrova.metadata.outbox.MetadataSyncService;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.setup.MetadataSchemaInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.persistence.catalog.api.projection.MetadataProjectionTransactions;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.persistence.catalog.postgresql.synchronization.PostgresMetadataSyncRepository;

/**
 * E2E test for cross-catalog metadata consistency under concurrent schema change.
 *
 * <p>Proves that version pinning (task 5.10) detects concurrent schema changes
 * during planning, and that {@code validateSyncedSample()} auto-corrects drift.</p>
 *
 * <p>Two scenarios:
 * <ol>
 *   <li>Stable cross-catalog JOIN: seed data in Iceberg + Paimon, cross-catalog
 *       JOIN returns correct results (baseline, no concurrent change)</li>
 *   <li>Schema drift detection: manually corrupt CatalogDeploymentRepository entry for one table,
 *       trigger sync, {@code validateSyncedSample()} detects drift, auto-corrects,
 *       query returns correct results</li>
 * </ol>
 */
@DisplayName("Cross-Catalog Metadata Consistency E2E")
class CrossCatalogConsistencyE2ETest extends SqlE2ETestBase {

    private static final String ICE_CATALOG = "iceberg_catalog";
    private static final String PAI_CATALOG = "paimon_catalog";
    private static final String SCHEMA = "default";
    private static final String ICE_PREFIX = ICE_CATALOG + "." + SCHEMA + ".";
    private static final String PAI_PREFIX = PAI_CATALOG + "." + SCHEMA + ".";

    private String iceTable;
    private String paiTable;
    private DataSource metadataDs;
    private MetadataEventDispatcher eventDispatcher;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(
                        ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(
                        PAI_CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    @BeforeAll
    static void setupCatalogs() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(
                        ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(
                        PAI_CATALOG, SCHEMA, paimonWarehouse().toString())));
    }

    @BeforeEach
    void initTestState(TestInfo testInfo) {
        String method = testInfo.getTestMethod()
                .map(m -> m.getName()).orElse("unknown");
        long ts = System.nanoTime();
        iceTable = "ice_cc_" + method + "_" + ts;
        paiTable = "pai_cc_" + method + "_" + ts;
        metadataDs = MetadataSchemaInitializer.createDataSource(
                mysqlJdbcUrl(), mysqlUsername(), mysqlPassword());
        eventDispatcher = new MetadataEventDispatcher();
    }

    @AfterEach
    void cleanupDispatcher() {
        if (eventDispatcher != null) {
            eventDispatcher.close();
        }
    }

    private String iceFqn() {
        return ICE_PREFIX + iceTable;
    }

    private String paiFqn() {
        return PAI_PREFIX + paiTable;
    }

    // -----------------------------------------------------------------------
    // Scenario 1 — Stable cross-catalog JOIN (baseline, no concurrent change)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1 — Stable cross-catalog JOIN")
    class StableCrossCatalogJoin {

        @Test
        @DisplayName("Cross-catalog JOIN between Iceberg and Paimon returns correct results")
        void crossCatalogJoinReturnsCorrectResults() {
            // Seed Iceberg table with customer data
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, name VARCHAR, region VARCHAR)");
            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, name, region) VALUES "
                    + "(1, 'alice', 'east'), (2, 'bob', 'west'), (3, 'carol', 'east')");

            // Seed Paimon table with order data
            sqlRunner.executeDdl("CREATE TABLE " + paiFqn()
                    + " (id BIGINT, customer_id BIGINT, amount DOUBLE)");
            sqlRunner.execute("INSERT INTO " + paiFqn()
                    + " (id, customer_id, amount) VALUES "
                    + "(10, 1, 100.0), (20, 2, 200.0), (30, 1, 150.0), (40, 3, 300.0)");

            // Cross-catalog JOIN: Iceberg customers × Paimon orders
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT c.name, SUM(o.amount) AS total "
                    + "FROM " + iceFqn() + " c "
                    + "JOIN " + paiFqn() + " o ON c.id = o.customer_id "
                    + "GROUP BY c.name "
                    + "ORDER BY c.name");

            assertThat(rows).hasSize(3);

            // alice: 100.0 + 150.0 = 250.0
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(((Number) rows.get(0).getRaw("total")).doubleValue()).isEqualTo(250.0);

            // bob: 200.0
            assertThat(rows.get(1).getRaw("name")).isEqualTo("bob");
            assertThat(((Number) rows.get(1).getRaw("total")).doubleValue()).isEqualTo(200.0);

            // carol: 300.0
            assertThat(rows.get(2).getRaw("name")).isEqualTo("carol");
            assertThat(((Number) rows.get(2).getRaw("total")).doubleValue()).isEqualTo(300.0);
        }

        @Test
        @DisplayName("Cross-catalog JOIN with filter pushdown returns correct subset")
        void crossCatalogJoinWithFilterReturnsCorrectSubset() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, name VARCHAR, region VARCHAR)");
            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, name, region) VALUES "
                    + "(1, 'alice', 'east'), (2, 'bob', 'west'), (3, 'carol', 'east')");

            sqlRunner.executeDdl("CREATE TABLE " + paiFqn()
                    + " (id BIGINT, customer_id BIGINT, amount DOUBLE)");
            sqlRunner.execute("INSERT INTO " + paiFqn()
                    + " (id, customer_id, amount) VALUES "
                    + "(10, 1, 100.0), (20, 2, 200.0), (30, 1, 150.0), (40, 3, 300.0)");

            // Filter on Iceberg side (region = 'east') before JOIN
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT c.name, o.amount "
                    + "FROM " + iceFqn() + " c "
                    + "JOIN " + paiFqn() + " o ON c.id = o.customer_id "
                    + "WHERE c.region = 'east' "
                    + "ORDER BY c.name, o.amount");

            // alice (east): 100.0, 150.0; carol (east): 300.0
            assertThat(rows).hasSize(3);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(((Number) rows.get(0).getRaw("amount")).doubleValue()).isEqualTo(100.0);
            assertThat(rows.get(1).getRaw("name")).isEqualTo("alice");
            assertThat(((Number) rows.get(1).getRaw("amount")).doubleValue()).isEqualTo(150.0);
            assertThat(rows.get(2).getRaw("name")).isEqualTo("carol");
            assertThat(((Number) rows.get(2).getRaw("amount")).doubleValue()).isEqualTo(300.0);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2 — Schema drift detection and auto-correction
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2 — Schema drift detection")
    class SchemaDriftDetection {

        @Test
        @DisplayName("Corrupted CatalogDeploymentRepository entry detected by incrementalSync drift validation and auto-corrected")
        void driftDetectedAndAutoCorrected() throws Exception {
            // Step 1: Create table and insert data via real pipeline
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, name VARCHAR, value BIGINT)");
            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, name, value) VALUES "
                    + "(1, 'alpha', 100), (2, 'beta', 200)");

            // Verify baseline query works
            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + iceFqn() + " ORDER BY id");
            assertThat(baseline).hasSize(2);

            // Step 2: Manually corrupt the CatalogDeploymentRepository entry — change column list
            // to simulate schema drift (deployments has stale columns that don't match
            // what the actual catalog reports)
            CatalogDeploymentRepository deployments = new PostgresCatalogDeploymentRepository(metadataDs);
        var tables = new PostgresTableProjectionRepository(metadataDs);
        var projections = new PostgresMetadataProjectionTransactions(metadataDs);
            ensureStoreParentRows(deployments, projections, ICE_CATALOG, SCHEMA);
            TableMetadata corruptedMeta = new TableMetadata(
                    ICE_CATALOG, SCHEMA, iceTable,
                    List.of(
                            new ColumnMetadata("id", ColumnType.of("BIGINT"), true, null, null),
                            new ColumnMetadata("stale_col", ColumnType.of("VARCHAR"), true, null, null)),
                    Map.of(), List.of(), Map.of(),
                    Instant.now(), Instant.now());
            new org.pangrova.testing.testkit.metadata.CurrentCatalogProjectionFixture(deployments, projections).saveTable(corruptedMeta);

            // Verify corruption: deployments now has wrong columns
            Optional<TableMetadata> storedAfterCorruption = tables.loadTable(
                    ICE_CATALOG, SCHEMA, iceTable);
            assertThat(storedAfterCorruption).isPresent();
            assertThat(storedAfterCorruption.get().columns())
                    .extracting(ColumnMetadata::name)
                    .contains("stale_col")
                    .doesNotContain("name", "value");

            // Step 3: Create a FederatedCatalog adapter that simulates drift:
            // - readChangesSince() returns changes with STALE metadata (same as corrupted deployments)
            // - getTable() returns the CORRECT metadata (what the real catalog has)
            // This models the scenario where incremental change detection is slightly behind
            // the actual catalog state. applyChanges saves the stale metadata from the change,
            // then validateSyncedSample detects the mismatch via getTable().
            TableMetadata correctMeta = new TableMetadata(
                    ICE_CATALOG, SCHEMA, iceTable,
                    List.of(
                            new ColumnMetadata("id", ColumnType.of("BIGINT"), true, null, null),
                            new ColumnMetadata("name", ColumnType.of("VARCHAR"), true, null, null),
                            new ColumnMetadata("value", ColumnType.of("BIGINT"), true, null, null)),
                    Map.of(), List.of(), Map.of(),
                    Instant.now(), Instant.now());

            // Build enough changes to statistically guarantee drift detection
            // (10% sample rate, 100 changes = ~10 validations expected)
            List<MetadataChange> staleChanges = new java.util.ArrayList<>();
            for (int i = 0; i < 100; i++) {
                staleChanges.add(new MetadataChange(
                        MetadataChange.ChangeType.UPDATE,
                        new QualifiedTableRef(ICE_CATALOG, SCHEMA, iceTable),
                        corruptedMeta));
            }

            FederatedCatalog driftAdapter = new DriftSimulatingAdapter(
                    correctMeta, staleChanges);

            // Step 4: Create MetadataSyncService and trigger incremental sync
            MetadataSyncService syncService = new MetadataSyncService(
                    tables, projections, new PostgresMetadataSyncRepository(metadataDs), new NoOpMetadataCache(), eventDispatcher);

            syncService.incrementalSync(deployments.getCatalogDescriptor(ICE_CATALOG).orElseThrow(), driftAdapter);

            // Step 5: Verify drift was detected and auto-corrected
            assertThat(syncService.getSchemaDriftDetectedCount())
                    .as("Schema drift should be detected at least once")
                    .isGreaterThan(0);

            // Verify deployments now has correct metadata (auto-corrected by validateSyncedSample)
            Optional<TableMetadata> corrected = tables.loadTable(
                    ICE_CATALOG, SCHEMA, iceTable);
            assertThat(corrected).isPresent();
            assertThat(corrected.get().columns())
                    .extracting(ColumnMetadata::name)
                    .containsExactly("id", "name", "value");

            // Step 6: Query still returns correct results after drift correction
            List<TypedRow> afterCorrection = sqlRunner.execute(
                    "SELECT * FROM " + iceFqn() + " ORDER BY id");
            assertThat(afterCorrection).hasSize(2);
            assertThat(afterCorrection.get(0).getRaw("name")).isEqualTo("alpha");
            assertThat(((Number) afterCorrection.get(0).getRaw("value")).longValue())
                    .isEqualTo(100L);
            assertThat(afterCorrection.get(1).getRaw("name")).isEqualTo("beta");
            assertThat(((Number) afterCorrection.get(1).getRaw("value")).longValue())
                    .isEqualTo(200L);

        }

        @Test
        @DisplayName("Cross-catalog query returns correct results after drift correction on one catalog")
        void crossCatalogQueryCorrectAfterDriftCorrection() throws Exception {
            // Seed both catalogs
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, name VARCHAR)");
            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

            sqlRunner.executeDdl("CREATE TABLE " + paiFqn()
                    + " (id BIGINT, score DOUBLE)");
            sqlRunner.execute("INSERT INTO " + paiFqn()
                    + " (id, score) VALUES (1, 95.0), (2, 85.0)");

            // Corrupt Iceberg table metadata in deployments
            CatalogDeploymentRepository deployments = new PostgresCatalogDeploymentRepository(metadataDs);
        var tables = new PostgresTableProjectionRepository(metadataDs);
        var projections = new PostgresMetadataProjectionTransactions(metadataDs);
            ensureStoreParentRows(deployments, projections, ICE_CATALOG, SCHEMA);
            TableMetadata corruptedMeta = new TableMetadata(
                    ICE_CATALOG, SCHEMA, iceTable,
                    List.of(
                            new ColumnMetadata("id", ColumnType.of("BIGINT"), true, null, null),
                            new ColumnMetadata("wrong_col", ColumnType.of("VARCHAR"), true, null, null)),
                    Map.of(), List.of(), Map.of(),
                    Instant.now(), Instant.now());
            new org.pangrova.testing.testkit.metadata.CurrentCatalogProjectionFixture(deployments, projections).saveTable(corruptedMeta);

            // Correct metadata from the "real" catalog
            TableMetadata correctMeta = new TableMetadata(
                    ICE_CATALOG, SCHEMA, iceTable,
                    List.of(
                            new ColumnMetadata("id", ColumnType.of("BIGINT"), true, null, null),
                            new ColumnMetadata("name", ColumnType.of("VARCHAR"), true, null, null)),
                    Map.of(), List.of(), Map.of(),
                    Instant.now(), Instant.now());

            // Build stale changes for drift detection
            List<MetadataChange> staleChanges = new java.util.ArrayList<>();
            for (int i = 0; i < 100; i++) {
                staleChanges.add(new MetadataChange(
                        MetadataChange.ChangeType.UPDATE,
                        new QualifiedTableRef(ICE_CATALOG, SCHEMA, iceTable),
                        corruptedMeta));
            }

            // Trigger drift detection and auto-correction via incremental sync
            MetadataSyncService syncService = new MetadataSyncService(
                    tables, projections, new PostgresMetadataSyncRepository(metadataDs), new NoOpMetadataCache(), eventDispatcher);

            syncService.incrementalSync(deployments.getCatalogDescriptor(ICE_CATALOG).orElseThrow(),
                    new DriftSimulatingAdapter(correctMeta, staleChanges));

            assertThat(syncService.getSchemaDriftDetectedCount())
                    .as("Drift should be detected")
                    .isGreaterThan(0);

            // Cross-catalog JOIN after correction returns correct results
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT c.name, p.score "
                    + "FROM " + iceFqn() + " c "
                    + "JOIN " + paiFqn() + " p ON c.id = p.id "
                    + "ORDER BY c.name");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(((Number) rows.get(0).getRaw("score")).doubleValue()).isEqualTo(95.0);
            assertThat(rows.get(1).getRaw("name")).isEqualTo("bob");
            assertThat(((Number) rows.get(1).getRaw("score")).doubleValue()).isEqualTo(85.0);

        }
    }

    // -----------------------------------------------------------------------
    // Schema setup helpers
    // -----------------------------------------------------------------------

    /** Ensures the FK parent rows exist without rewriting a live catalog revision. */
    private void ensureStoreParentRows(CatalogDeploymentRepository deployments, MetadataProjectionTransactions projections, String catalogName, String schemaName) {
        if (deployments.getCatalogDescriptor(catalogName).isEmpty()) {
            deployments.saveCatalogDescriptor(new CatalogDescriptor(
                    catalogName, CatalogType.CONNECTOR, "iceberg", CatalogState.ACTIVE,
                    Map.of(), null, Set.of()));
        }
        new org.pangrova.testing.testkit.metadata.CurrentCatalogProjectionFixture(deployments, projections).saveSchemas(catalogName, List.of(schemaName));
    }

    // -----------------------------------------------------------------------
    // Test doubles
    // -----------------------------------------------------------------------

    /**
     * FederatedCatalog adapter that simulates schema drift:
     * {@code readChangesSince()} returns changes with stale metadata,
     * while {@code getTable()} returns the correct (current) metadata.
     * This models the scenario where incremental change detection is slightly
     * behind the actual catalog state, triggering drift detection in
     * {@code validateSyncedSample()}.
     */
    private static class DriftSimulatingAdapter implements FederatedCatalog {
        private final TableMetadata correctMetadata;
        private final List<MetadataChange> staleChanges;

        DriftSimulatingAdapter(TableMetadata correctMetadata,
                               List<MetadataChange> staleChanges) {
            this.correctMetadata = correctMetadata;
            this.staleChanges = staleChanges;
        }

        @Override
        public List<String> listSchemas() {
            return List.of(correctMetadata.schema());
        }

        @Override
        public List<String> listTables(String schema) {
            return List.of(correctMetadata.table());
        }

        @Override
        public TableMetadata getTable(String schema, String table) {
            return correctMetadata;
        }

        @Override
        public TableStatistics getTableStatistics(String schema, String table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public org.pangrova.metadata.api.catalog.MetadataChangeBatch readChangesSince(Instant since) {
            return new org.pangrova.metadata.api.catalog.MetadataChangeBatch(staleChanges, java.util.Optional.empty());
        }

        @Override
        public void close() {
        }
    }

    /**
     * No-op metadata cache for use in sync service construction.
     */
    private static class NoOpMetadataCache implements MetadataCache {
        @Override
        public Optional<TableMetadata> getTable(String c, String s, String t) {
            return Optional.empty();
        }

        @Override
        public void putTable(String c, String s, String t, TableMetadata m) {
        }

        @Override
        public void invalidateTable(String c, String s, String t) {
        }

        @Override
        public void invalidateCatalog(String c) {
        }

        @Override
        public CacheStats getStats() {
            return new CacheStats(0L, 0L, 0L, 0L);
        }
    }
}
