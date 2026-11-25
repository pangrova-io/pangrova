package org.pangrova.indexing.evolution;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.descriptor.IndexSchemaFingerprint;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.evolution.SchemaChangeEvent;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaEvolutionRecord;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaTransition;
import org.pangrova.persistence.indexing.api.enums.IndexSchemaEvolutionState;
import org.pangrova.types.enums.PangrovaType;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class IndexSchemaEvolutionHandlerTest {
    private static final Instant NOW = Instant.parse("2026-11-26T00:00:00Z");

    @Test
    void compatibleRenameRebindsColumnsAndFingerprintUnderTheExistingFence() {
        var descriptor = descriptor(IndexType.BLOOM_FILTER, "value", PangrovaType.BIGINT);
        var event = SchemaChangeEvent.renameColumn(
                "tenant", "lake", "raw", "items", "value", "renamed");
        var transitions = apply(event, descriptor, metadata("renamed", PangrovaType.BIGINT));

        assertThat(transitions).singleElement().satisfies(transition -> {
            assertThat(transition.newState()).isEqualTo(IndexState.STALE);
            assertThat(transition.newColumnNames()).containsExactly("renamed");
            assertThat(transition.sourceSchemaFingerprint()).isNotEqualTo(descriptor.sourceSchemaFingerprint());
            assertThat(transition.expectedUpdatedAt()).isEqualTo(descriptor.updatedAt());
            assertThat(transition.expectedGeneration()).isZero();
        });
    }

    @Test
    void incompatibleTextTypeChangeDropsTheIndexInsteadOfPublishingIt() {
        var descriptor = descriptor(IndexType.INVERTED, "value", PangrovaType.VARCHAR);
        var event = SchemaChangeEvent.modifyColumnType(
                "tenant", "lake", "raw", "items", "value", "VARCHAR", "BIGINT");
        var transitions = apply(event, descriptor, metadata("value", PangrovaType.BIGINT));

        assertThat(transitions).singleElement()
                .satisfies(transition -> assertThat(transition.newState()).isEqualTo(IndexState.DROPPED));
    }

    @Test
    void crossSchemaRenameRebindsBothDescriptorCoordinates() {
        var descriptor = descriptor(IndexType.BLOOM_FILTER, "value", PangrovaType.BIGINT);
        var event = SchemaChangeEvent.renameTable(
                "tenant", "lake", "raw", "items", "curated", "moved");
        var source = new ConnectorTableMetadata(
                new SchemaTableName("curated", "moved"), List.of("value"),
                List.of(new ConnectorTableMetadata.Column("value", PangrovaType.BIGINT, OptionalInt.of(1), null)),
                Map.of());

        var transitions = apply(event, descriptor, source);
        assertThat(transitions).hasSize(1);
        var transition = transitions.get(0);

        assertThat(transition.newState()).isEqualTo(IndexState.STALE);
        assertThat(transition.newSchemaName()).isEqualTo("curated");
        assertThat(transition.newTableName()).isEqualTo("moved");
    }

    @Test
    void uncertainDropKeepsTheExistingBindingWhenTheSourceStillExists() {
        var descriptor = descriptor(IndexType.BLOOM_FILTER, "value", PangrovaType.BIGINT);
        var event = SchemaChangeEvent.dropTable("tenant", "lake", "raw", "items");

        var transitions = apply(event, descriptor, metadata("value", PangrovaType.BIGINT));
        assertThat(transitions).hasSize(1);
        var transition = transitions.get(0);

        assertThat(transition.newState()).isEqualTo(IndexState.STALE);
        assertThat(transition.newSchemaName()).isEqualTo("raw");
        assertThat(transition.newTableName()).isEqualTo("items");
    }

    @Test
    void reconciliationFailureDoesNotPreventFencingAnotherIndex() {
        var first = descriptor("first", IndexType.BLOOM_FILTER, "value", PangrovaType.BIGINT);
        var second = descriptor("second", IndexType.BLOOM_FILTER, "value", PangrovaType.BIGINT);
        var fenced = new AtomicReference<String>();
        IndexMetadataStore store = (IndexMetadataStore) Proxy.newProxyInstance(
                IndexSchemaEvolutionHandlerTest.class.getClassLoader(),
                new Class<?>[] {IndexMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "findIndexesByState" -> List.of(first, second);
                    case "fenceIndexSchemaMismatch" -> {
                        fenced.set(((IndexDescriptor) arguments[0]).indexId());
                        yield Optional.of(UUID.randomUUID());
                    }
                    case "toString" -> "IndexMetadataStoreTestDouble";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new AssertionError(
                            "Unexpected IndexMetadataStore call: " + method.getName());
                });
        var handler = new IndexSchemaEvolutionHandler(store, (descriptor, event) -> {
            if (descriptor.indexId().equals("first")) {
                throw new IllegalStateException("connector unavailable");
            }
            return Optional.empty();
        });

        assertThat(handler.reconcile()).isOne();
        assertThat(fenced).hasValue("second");
    }

    private static List<IndexSchemaTransition> apply(
            SchemaChangeEvent event, IndexDescriptor descriptor, ConnectorTableMetadata source) {
        var record = new IndexSchemaEvolutionRecord(
                UUID.randomUUID(), null, event, IndexSchemaEvolutionState.COMMITTED, NOW, NOW);
        var transitions = new AtomicReference<List<IndexSchemaTransition>>();
        IndexMetadataStore store = (IndexMetadataStore) Proxy.newProxyInstance(
                IndexSchemaEvolutionHandlerTest.class.getClassLoader(),
                new Class<?>[] {IndexMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "listPendingSchemaEvolution" -> List.of(record);
                    case "findIndexesByTable" -> List.of(descriptor);
                    case "findIndex" -> Optional.of(descriptor);
                    case "findActiveIndexGeneration" -> Optional.empty();
                    case "applySchemaEvolution" -> {
                        @SuppressWarnings("unchecked")
                        List<IndexSchemaTransition> value =
                                (List<IndexSchemaTransition>) arguments[1];
                        transitions.set(value);
                        yield null;
                    }
                    case "toString" -> "IndexMetadataStoreTestDouble";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new AssertionError(
                            "Unexpected IndexMetadataStore call: " + method.getName());
                });
        var handler = new IndexSchemaEvolutionHandler(
                store, (ignoredDescriptor, ignoredEvent) -> Optional.of(source));

        assertThat(handler.recoverPending()).isOne();
        return transitions.get();
    }

    private static IndexDescriptor descriptor(IndexType type, String column, PangrovaType sourceType) {
        return descriptor("index-1", type, column, sourceType);
    }

    private static IndexDescriptor descriptor(
            String indexId, IndexType type, String column, PangrovaType sourceType) {
        var sourceColumn = new ConnectorTableMetadata.Column(
                column, sourceType, OptionalInt.of(1), null);
        return new IndexDescriptor(
                indexId, indexId + "_name", "lake", "raw", "items", List.of(column), type,
                Map.of(IndexDescriptor.SOURCE_SCHEMA_FINGERPRINT_PROPERTY,
                        IndexSchemaFingerprint.of(List.of(column), List.of(sourceColumn))),
                IndexState.STALE, "schema_change_pending", 0, "snapshot-1", "snapshot-1",
                NOW, NOW, "tenant");
    }

    private static ConnectorTableMetadata metadata(String column, PangrovaType type) {
        return new ConnectorTableMetadata(
                new SchemaTableName("raw", "items"), List.of(column),
                List.of(new ConnectorTableMetadata.Column(column, type, OptionalInt.of(1), null)),
                Map.of());
    }
}
