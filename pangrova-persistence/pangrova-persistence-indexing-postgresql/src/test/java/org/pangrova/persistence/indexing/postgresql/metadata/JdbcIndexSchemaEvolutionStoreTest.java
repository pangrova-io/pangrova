package org.pangrova.persistence.indexing.postgresql.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.TableIdentity;
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
        assertThat(store.listPendingSchemaEvolution(10)).isEmpty();
        assertThat(store.findSchemaEvolution(operationId)).hasValueSatisfying(record -> {
            assertThat(record.operationId()).isEqualTo(operationId);
            assertThat(record.state()).isEqualTo(IndexSchemaEvolutionState.INTENDED);
        });
        store.markSchemaEvolutionCommitted(operationId, NOW.plusSeconds(1));
        assertThat(store.listPendingSchemaEvolution(10)).singleElement()
                .satisfies(record -> assertThat(record.state()).isEqualTo(IndexSchemaEvolutionState.COMMITTED));
    }

    @Test
    void committedAndUnresolvedQueuesPreserveTheirIdentityAndOrdering() {
        var source = new TableIdentity("ICEBERG", "table-uuid", "warehouse/items", "schema-before");
        var target = new TableIdentity("ICEBERG", "table-uuid", "warehouse/moved", "schema-after");
        var rename = SchemaChangeEvent.renameTable("tenant", "lake", "raw", "items", "curated", "moved");
        UUID committed = store.prepareSchemaEvolution(rename, Optional.of(source), NOW);
        UUID intended = store.prepareSchemaEvolution(SchemaChangeEvent.dropTable(
                "tenant", "lake", "raw", "independent"), NOW.plusSeconds(1));
        UUID unknown = store.prepareSchemaEvolution(SchemaChangeEvent.dropTable(
                "tenant", "lake", "raw", "another"), NOW.plusSeconds(2));
        store.markSchemaEvolutionUnknown(unknown, NOW.plusSeconds(3));
        store.markSchemaEvolutionCommitted(committed, Optional.of(target), NOW.plusSeconds(4));

        assertThat(store.listPendingSchemaEvolution(1)).singleElement().satisfies(record -> {
            assertThat(record.operationId()).isEqualTo(committed);
            assertThat(record.sourceIdentity()).contains(source);
            assertThat(record.targetIdentity()).contains(target);
        });
        assertThat(store.listUnresolvedSchemaEvolution(1)).singleElement().satisfies(record -> {
            assertThat(record.operationId()).isEqualTo(intended);
            assertThat(record.sourceIdentity()).isEmpty();
            assertThat(record.targetIdentity()).isEmpty();
        });
        assertThat(store.listUnresolvedSchemaEvolution(2)).extracting(record -> record.operationId())
                .containsExactly(intended, unknown);
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

        UUID mismatch = store.fenceIndexSchemaMismatch(ready, "source_schema_mismatch", NOW).orElseThrow();
        assertThat(store.findIndex("index-1").orElseThrow().state()).isEqualTo(IndexState.STALE);
        assertThat(store.listPendingSchemaEvolution(10)).isEmpty();
        assertThat(store.findSchemaEvolution(mismatch)).hasValueSatisfying(record -> {
            assertThat(record.indexId()).isEqualTo("index-1");
            assertThat(record.state()).isEqualTo(IndexSchemaEvolutionState.APPLIED);
        });
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

    @Test
    void explicitStaleRebuildCanBindTheCurrentSchemaAfterTheScopeIsFree() {
        store.registerIndex(descriptor());
        store.fenceIndexSchemaMismatch(descriptor(), "source_schema_mismatch", NOW);
        var stale = store.findIndex("index-1").orElseThrow();
        String currentFingerprint = "v2:" + "b".repeat(64);
        assertThat(store.beginIndexGeneration(stale.indexId(), 1, "snapshot-2", NOW.plusSeconds(1),
                stale.updatedAt(), stale.state(), stale.generation(), currentFingerprint)).isTrue();
        assertThat(store.findIndex(stale.indexId()).orElseThrow().sourceSchemaFingerprint())
                .isEqualTo(currentFingerprint);
    }

    @Test
    void unknownOutcomeKeepsSourceAndRenameTargetOwnedUntilVerified() {
        store.registerIndex(descriptor());
        UUID operation = store.prepareSchemaEvolution(SchemaChangeEvent.renameTable(
                "tenant", "lake", "raw", "items", "curated", "moved"), NOW);
        store.markSchemaEvolutionUnknown(operation, NOW.plusSeconds(1));
        assertThat(store.findSchemaEvolution(operation)).hasValueSatisfying(record ->
                assertThat(record.state()).isEqualTo(IndexSchemaEvolutionState.UNKNOWN));
        assertThat(store.listPendingSchemaEvolution(10)).isEmpty();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.prepareSchemaEvolution(
                SchemaChangeEvent.dropTable("tenant", "lake", "curated", "moved"), NOW.plusSeconds(2)))
                .isInstanceOf(IllegalStateException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.prepareSchemaEvolution(
                SchemaChangeEvent.dropTable("tenant", "lake", "raw", "items"), NOW.plusSeconds(2)))
                .isInstanceOf(IllegalStateException.class);
        IndexDescriptor fenced = store.findIndex("index-1").orElseThrow();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.beginIndexGeneration(
                fenced.indexId(), 1, "snapshot-2", NOW.plusSeconds(3), fenced.updatedAt(),
                fenced.state(), fenced.generation(), FINGERPRINT)).isInstanceOf(RuntimeException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.registerIndex(new IndexDescriptor(
                "index-2", "items_other", "lake", "raw", "items", List.of("value"),
                IndexType.BLOOM_FILTER, Map.of(), IndexState.BUILDING, null, 0, "snapshot-2",
                null, NOW, NOW, "tenant"))).hasStackTraceContaining(operation.toString());
        assertThat(store.prepareSchemaEvolution(SchemaChangeEvent.dropTable(
                "tenant", "lake", "raw", "independent"), NOW.plusSeconds(3))).isNotNull();
        assertThat(store.findIndex("index-1").orElseThrow().state()).isEqualTo(IndexState.STALE);
    }

    @Test
    void completedOperationReleasesItsExactScopeButFencesOldStagingGeneration() {
        store.registerIndex(descriptor());
        IndexDescriptor initial = store.findIndex("index-1").orElseThrow();
        assertThat(store.beginIndexGeneration(initial.indexId(), 1, "snapshot-2", NOW.plusSeconds(1),
                initial.updatedAt(), initial.state(), initial.generation(), FINGERPRINT)).isTrue();
        UUID operation = store.prepareSchemaEvolution(
                SchemaChangeEvent.addColumn("tenant", "lake", "raw", "items", "other", "BIGINT"),
                NOW.plusSeconds(2));
        assertThat(store.isIndexGenerationBuildable(initial.indexId(), 1)).isFalse();
        store.markSchemaEvolutionCommitted(operation, NOW.plusSeconds(3));
        store.applySchemaEvolution(operation, List.of(), NOW.plusSeconds(4));
        assertThat(store.findSchemaEvolution(operation)).hasValueSatisfying(record ->
                assertThat(record.state()).isEqualTo(IndexSchemaEvolutionState.APPLIED));
        assertThat(store.isIndexGenerationBuildable(initial.indexId(), 1)).isFalse();
        assertThat(store.prepareSchemaEvolution(
                SchemaChangeEvent.dropTable("tenant", "lake", "raw", "items"), NOW.plusSeconds(5))).isNotNull();
    }

    @Test
    void concurrentTablePreparationHasOneDurableOwner() throws Exception {
        store.registerIndex(descriptor());
        var second = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        var workers = java.util.concurrent.Executors.newFixedThreadPool(2);
        var start = new java.util.concurrent.CountDownLatch(1);
        var event = SchemaChangeEvent.dropTable("tenant", "lake", "raw", "items");
        try {
            java.util.concurrent.Callable<Optional<UUID>> prepareFirst = () -> {
                start.await();
                try {
                    return Optional.of(store.prepareSchemaEvolution(event, NOW));
                } catch (IllegalStateException alreadyOwned) {
                    return Optional.empty();
                }
            };
            java.util.concurrent.Callable<Optional<UUID>> prepareSecond = () -> {
                start.await();
                try {
                    return Optional.of(second.prepareSchemaEvolution(event, NOW));
                } catch (IllegalStateException alreadyOwned) {
                    return Optional.empty();
                }
            };
            var first = workers.submit(prepareFirst);
            var another = workers.submit(prepareSecond);
            start.countDown();
            var admitted = java.util.stream.Stream.of(first.get(10, java.util.concurrent.TimeUnit.SECONDS),
                    another.get(10, java.util.concurrent.TimeUnit.SECONDS)).flatMap(Optional::stream).toList();
            assertThat(admitted).hasSize(1);
            assertThat(store.listUnresolvedSchemaEvolution(10)).singleElement().satisfies(record ->
                    assertThat(record.operationId()).isEqualTo(admitted.get(0)));
            assertThat(store.findIndex("index-1").orElseThrow().state()).isEqualTo(IndexState.STALE);
        } finally {
            workers.shutdownNow();
            assertThat(workers.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void confirmedPreMutationAbortRetainsStaleDescriptorAndUnresolvedInspectionIsReadOnly() {
        store.registerIndex(descriptor());
        var event = SchemaChangeEvent.dropTable("tenant", "lake", "raw", "items");
        UUID noMutation = store.prepareSchemaEvolution(event, NOW);
        store.markSchemaEvolutionAborted(noMutation, NOW.plusSeconds(1));
        assertThat(store.findSchemaEvolution(noMutation)).hasValueSatisfying(record ->
                assertThat(record.state()).isEqualTo(IndexSchemaEvolutionState.ABORTED));
        IndexDescriptor stale = store.findIndex("index-1").orElseThrow();
        assertThat(stale.state()).isEqualTo(IndexState.STALE);

        UUID unresolved = store.prepareSchemaEvolution(event, NOW.plusSeconds(2));
        store.markSchemaEvolutionUnknown(unresolved, NOW.plusSeconds(3));
        assertThat(store.listUnresolvedSchemaEvolution(26)).singleElement()
                .satisfies(record -> assertThat(record.operationId()).isEqualTo(unresolved));
        assertThat(store.listUnresolvedSchemaEvolution(26)).singleElement()
                .satisfies(record -> assertThat(record.state()).isEqualTo(IndexSchemaEvolutionState.UNKNOWN));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.prepareSchemaEvolution(event, NOW.plusSeconds(4)))
                .hasMessageContaining(unresolved.toString());
        assertThat(store.findIndex("index-1").orElseThrow().state()).isEqualTo(IndexState.STALE);
    }

    @Test
    void confirmedPreConnectorAbortReleasesScopeButUnknownCannotBeAborted() {
        var event = SchemaChangeEvent.dropTable("tenant", "lake", "raw", "items");
        UUID beforeCall = store.prepareSchemaEvolution(event, NOW);
        store.markSchemaEvolutionAborted(beforeCall, NOW.plusSeconds(1));
        assertThat(store.findSchemaEvolution(beforeCall)).hasValueSatisfying(record ->
                assertThat(record.state()).isEqualTo(IndexSchemaEvolutionState.ABORTED));
        UUID uncertain = store.prepareSchemaEvolution(event, NOW.plusSeconds(2));
        store.markSchemaEvolutionUnknown(uncertain, NOW.plusSeconds(3));
        store.markSchemaEvolutionAborted(uncertain, NOW.plusSeconds(4));
        assertThat(store.findSchemaEvolution(uncertain)).hasValueSatisfying(record ->
                assertThat(record.state()).isEqualTo(IndexSchemaEvolutionState.UNKNOWN));
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                store.prepareSchemaEvolution(event, NOW.plusSeconds(5))).hasMessageContaining(uncertain.toString());
    }

    @Test
    void lostPrepareCommitResponseReleasesOnlyProvablyPreConnectorClaim() {
        var lost = new java.util.concurrent.atomic.AtomicBoolean(false);
        DataSource responseLoss = (DataSource) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {DataSource.class}, (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(dataSource, args);
                        if ("getConnection".equals(method.getName()) && result instanceof java.sql.Connection physical) {
                            return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                                    new Class<?>[] {java.sql.Connection.class}, (connection, call, values) -> {
                                        try {
                                            Object value = call.invoke(physical, values);
                                            if ("commit".equals(call.getName()) && lost.compareAndSet(false, true)) {
                                                throw new java.sql.SQLException("simulated lost prepare commit response");
                                            }
                                            return value;
                                        } catch (java.lang.reflect.InvocationTargetException failure) {
                                            throw failure.getCause();
                                        }
                                    });
                        }
                        return result;
                    } catch (java.lang.reflect.InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
        var recovering = new JdbcIndexMetadataStore(responseLoss, TableWriteAuthorityFixture.create()::requireCurrent);
        var event = SchemaChangeEvent.dropTable("tenant", "lake", "raw", "items");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> recovering.prepareSchemaEvolution(event, NOW))
                .hasStackTraceContaining("simulated lost prepare commit response");
        assertThat(lost.get()).isTrue();
        assertThat(store.listUnresolvedSchemaEvolution(10)).isEmpty();
        // The prepare never returned an operation ID; consequently no connector
        // DDL could start, and a fresh operation may safely own this table.
        assertThat(store.prepareSchemaEvolution(event, NOW.plusSeconds(1))).isNotNull();
    }

    private static IndexDescriptor descriptor() {
        return new IndexDescriptor(
                "index-1", "items_value", "lake", "raw", "items", List.of("value"),
                IndexType.BLOOM_FILTER,
                Map.of(IndexDescriptor.SOURCE_SCHEMA_FINGERPRINT_PROPERTY, FINGERPRINT),
                IndexState.READY, null, 0, "snapshot-1", "snapshot-1", NOW, NOW, "tenant");
    }
}
