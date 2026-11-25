package org.pangrova.persistence.indexing.postgresql.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.evolution.SchemaChangeEvent;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaTransition;
import org.pangrova.persistence.indexing.api.enums.IndexSchemaEvolutionState;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcIndexSchemaEvolutionStoreTest extends ContainerTestBase {
    private static final Instant NOW = Instant.parse("2026-11-26T00:00:00Z");
    private static final String FINGERPRINT = "a".repeat(64);

    private DataSource dataSource;
    private JdbcIndexMetadataStore store;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource(
                "index_schema_evolution_" + UUID.randomUUID().toString().replace("-", ""));
        store = new JdbcIndexMetadataStore(
                dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
    }

    @Test
    void writeAheadFencePrecedesConnectorMutationAndSurvivesRestart() {
        store.registerIndex(descriptor());

        UUID operationId = store.prepareSchemaEvolution(
                SchemaChangeEvent.dropColumn("tenant", "lake", "raw", "items", "value"), NOW);

        assertThat(store.findIndex("index-1").orElseThrow().state()).isEqualTo(IndexState.STALE);
        assertThat(store.listPendingSchemaEvolution(10)).singleElement().satisfies(record -> {
            assertThat(record.operationId()).isEqualTo(operationId);
            assertThat(record.state()).isEqualTo(IndexSchemaEvolutionState.INTENDED);
        });
        store.markSchemaEvolutionCommitted(operationId, NOW.plusSeconds(1));
        assertThat(store.listPendingSchemaEvolution(10)).singleElement()
                .satisfies(record -> assertThat(record.state()).isEqualTo(IndexSchemaEvolutionState.COMMITTED));
    }

    @Test
    void committedTransitionIsAtomicAndReplayCannotResurrectADroppedIndex() {
        store.registerIndex(descriptor());
        UUID operationId = store.prepareSchemaEvolution(
                SchemaChangeEvent.dropColumn("tenant", "lake", "raw", "items", "value"), NOW);
        store.markSchemaEvolutionCommitted(operationId, NOW.plusSeconds(1));
        IndexDescriptor fenced = store.findIndex("index-1").orElseThrow();
        var transition = new IndexSchemaTransition(
                fenced.indexId(), fenced.catalogName(), fenced.schemaName(), IndexState.DROPPED,
                fenced.tableName(), fenced.columnNames(), FINGERPRINT, null,
                fenced.updatedAt(), fenced.state(), fenced.generation(), fenced.schemaName());

        store.applySchemaEvolution(operationId, List.of(transition), NOW.plusSeconds(2));
        store.applySchemaEvolution(operationId, List.of(transition), NOW.plusSeconds(3));

        assertThat(store.findIndex("index-1").orElseThrow().state()).isEqualTo(IndexState.DROPPED);
        assertThat(store.listPendingSchemaEvolution(10)).isEmpty();
    }

    @Test
    void mismatchFenceRejectsAnAlreadySupersededDescriptor() {
        IndexDescriptor ready = descriptor();
        store.registerIndex(ready);

        assertThat(store.fenceIndexSchemaMismatch(ready, "source_schema_mismatch", NOW)).isPresent();
        assertThat(store.findIndex("index-1").orElseThrow().state()).isEqualTo(IndexState.STALE);
        assertThat(store.listPendingSchemaEvolution(10)).singleElement()
                .satisfies(record -> assertThat(record.indexId()).isEqualTo("index-1"));
        assertThat(store.fenceIndexSchemaMismatch(ready, "duplicate", NOW.plusSeconds(1))).isEmpty();
    }

    @Test
    void crossSchemaRenameTransitionRebindsTheDescriptorAtomically() {
        store.registerIndex(descriptor());
        UUID operationId = store.prepareSchemaEvolution(
                SchemaChangeEvent.renameTable("tenant", "lake", "raw", "items", "curated", "moved"),
                NOW);
        store.markSchemaEvolutionCommitted(operationId, NOW.plusSeconds(1));
        IndexDescriptor fenced = store.findIndex("index-1").orElseThrow();
        var transition = new IndexSchemaTransition(
                fenced.indexId(), fenced.catalogName(), fenced.schemaName(), IndexState.STALE,
                "moved", fenced.columnNames(), FINGERPRINT, "schema_evolution",
                fenced.updatedAt(), fenced.state(), fenced.generation(), "curated");

        store.applySchemaEvolution(operationId, List.of(transition), NOW.plusSeconds(2));

        var rebound = store.findIndex("index-1").orElseThrow();
        assertThat(rebound.schemaName()).isEqualTo("curated");
        assertThat(rebound.tableName()).isEqualTo("moved");
        assertThat(store.listPendingSchemaEvolution(10)).isEmpty();
    }

    @Test
    void schemaTransitionCannotOverwriteASupersedingBuildGeneration() throws Exception {
        store.registerIndex(descriptor());
        UUID operationId = store.prepareSchemaEvolution(
                SchemaChangeEvent.dropColumn("tenant", "lake", "raw", "items", "value"), NOW);
        store.markSchemaEvolutionCommitted(operationId, NOW.plusSeconds(1));
        IndexDescriptor fenced = store.findIndex("index-1").orElseThrow();
        var transition = new IndexSchemaTransition(
                fenced.indexId(), fenced.catalogName(), fenced.schemaName(), IndexState.DROPPED,
                fenced.tableName(), fenced.columnNames(), FINGERPRINT, null,
                fenced.updatedAt(), fenced.state(), fenced.generation(), fenced.schemaName());
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "UPDATE index_descriptors SET state='BUILDING',generation=1,updated_at=? WHERE index_id=?")) {
            statement.setObject(1, NOW.plusSeconds(2).atOffset(java.time.ZoneOffset.UTC));
            statement.setString(2, fenced.indexId());
            statement.executeUpdate();
        }

        store.applySchemaEvolution(operationId, List.of(transition), NOW.plusSeconds(3));

        var building = store.findIndex("index-1").orElseThrow();
        assertThat(building.state()).isEqualTo(IndexState.BUILDING);
        assertThat(building.generation()).isOne();
    }

    @Test
    void rebuildReservationBindsAPreviouslyUnboundSchema() {
        store.registerIndex(descriptor().withSourceSchemaFingerprint(null));
        IndexDescriptor unbound = store.findIndex("index-1").orElseThrow();

        assertThat(store.beginIndexGeneration(
                unbound.indexId(), 1, "snapshot-2", NOW.plusSeconds(1), unbound.updatedAt(),
                unbound.state(), unbound.generation(), FINGERPRINT)).isTrue();

        assertThat(store.findIndex("index-1").orElseThrow().sourceSchemaFingerprint()).isEqualTo(FINGERPRINT);
    }

    private static IndexDescriptor descriptor() {
        return new IndexDescriptor(
                "index-1", "items_value", "lake", "raw", "items", List.of("value"),
                IndexType.BLOOM_FILTER,
                Map.of(IndexDescriptor.SOURCE_SCHEMA_FINGERPRINT_PROPERTY, FINGERPRINT),
                IndexState.READY, null, 0, "snapshot-1", "snapshot-1", NOW, NOW, "tenant");
    }
}
