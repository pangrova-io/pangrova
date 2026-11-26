package org.pangrova.coordinator.indexing;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.enums.FieldIdStability;
import org.pangrova.connector.api.enums.ReadSnapshotState;
import org.pangrova.connector.api.enums.TableDeclarationEvidence;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorReadSchemaIdentity;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.descriptor.IndexSchemaFingerprint;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.planner.analysis.SplitSourceDescriptor;
import org.pangrova.types.enums.PangrovaType;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class IndexSchemaGuardTest {
    private static final Instant NOW = Instant.parse("2026-11-26T00:00:00Z");

    @Test
    void matchingSchemaUsesTheIndex() {
        var fetcher = fetcher(descriptor(), new AtomicReference<>());

        assertThat(fetcher.lookupIndexesForQuery(
                "tenant", "lake.raw.items", "value", List.of(source(PangrovaType.BIGINT, true))))
                .extracting(IndexDescriptor::indexId).containsExactly("index-1");
    }

    @Test
    void mismatchedSchemaSkipsTheIndexAndDurablyFencesIt() {
        var fenced = new AtomicReference<IndexDescriptor>();
        var fetcher = fetcher(descriptor(), fenced);

        assertThat(fetcher.lookupIndexesForQuery(
                "tenant", "lake.raw.items", "value", List.of(source(PangrovaType.VARCHAR, true))))
                .isEmpty();
        assertThat(fenced).hasValue(descriptor());
    }

    @Test
    void missingSchemaEvidenceFailsClosedWithoutFencing() {
        var fenced = new AtomicReference<IndexDescriptor>();
        var fetcher = fetcher(descriptor(), fenced);

        assertThat(fetcher.lookupIndexesForQuery(
                "tenant", "lake.raw.items", "value", List.of(source(PangrovaType.BIGINT, false))))
                .isEmpty();
        assertThat(fenced).hasValue(null);
    }

    @Test
    void ambiguousSelfJoinSchemasFailClosedWithoutChoosingTheFirstScan() {
        var fenced = new AtomicReference<IndexDescriptor>();
        var fetcher = fetcher(descriptor(), fenced);

        assertThat(fetcher.lookupIndexesForQuery(
                "tenant", "lake.raw.items", "value",
                List.of(source(PangrovaType.BIGINT, true), source(PangrovaType.VARCHAR, true))))
                .isEmpty();
        assertThat(fenced).hasValue(null);
    }

    @Test
    void missingIndexedColumnFencesTheDescriptor() {
        var fenced = new AtomicReference<IndexDescriptor>();
        var fetcher = fetcher(descriptor(), fenced);
        var otherColumn = new ConnectorTableMetadata.Column(
                "other", PangrovaType.BIGINT, OptionalInt.of(2), null);
        var metadata = new ConnectorTableMetadata(
                new SchemaTableName("raw", "items"), List.of("other"), List.of(otherColumn), Map.of());
        var identity = Optional.of(new ConnectorReadSchemaIdentity(
                new TableIdentity("TEST", "table-uuid", "file:///items", "schema-2"),
                "test-field-namespace", List.of(otherColumn), FieldIdStability.SELECTED_SOURCE));
        var binding = new ConnectorReadBinding(
                new ConnectorTableHandle() {}, new ConnectorTableReadMetadata(metadata, TableDeclarationEvidence.EXACT),
                TableStatistics.UNKNOWN, ReadSnapshotState.SNAPSHOT, OptionalLong.of(1), "source-2",
                identity, Optional.empty());
        var missingColumnSource = new SplitSourceDescriptor(
                "lake", "raw", "items", Constraint.EMPTY, List.of("other"), Optional.of(binding));

        assertThat(fetcher.lookupIndexesForQuery(
                "tenant", "lake.raw.items", "value", List.of(missingColumnSource))).isEmpty();
        assertThat(fenced).hasValue(descriptor());
    }

    private static IndexAwareSplitFetcher fetcher(
            IndexDescriptor descriptor, AtomicReference<IndexDescriptor> fenced) {
        IndexMetadataStore store = (IndexMetadataStore) Proxy.newProxyInstance(
                IndexSchemaGuardTest.class.getClassLoader(), new Class<?>[] {IndexMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "findIndexesByTable" -> List.of(descriptor);
                    case "fenceIndexSchemaMismatch" -> {
                        fenced.set((IndexDescriptor) arguments[0]);
                        yield Optional.of(java.util.UUID.randomUUID());
                    }
                    case "toString" -> "IndexMetadataStoreTestDouble";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new AssertionError(
                            "Unexpected IndexMetadataStore call: " + method.getName());
                });
        return new IndexAwareSplitFetcher(null, null, store, null);
    }

    private static SplitSourceDescriptor source(PangrovaType type, boolean withSchemaIdentity) {
        var column = new ConnectorTableMetadata.Column("value", type, OptionalInt.of(1), null);
        var metadata = new ConnectorTableMetadata(
                new SchemaTableName("raw", "items"), List.of("value"), List.of(column), Map.of());
        Optional<ConnectorReadSchemaIdentity> identity = withSchemaIdentity
                ? Optional.of(new ConnectorReadSchemaIdentity(
                        new TableIdentity("TEST", "table-uuid", "file:///items", "schema-1"),
                        "test-field-namespace", List.of(column), FieldIdStability.SELECTED_SOURCE))
                : Optional.empty();
        var declaration = new ConnectorTableReadMetadata(metadata, TableDeclarationEvidence.EXACT);
        var binding = new ConnectorReadBinding(
                new ConnectorTableHandle() {}, declaration, TableStatistics.UNKNOWN,
                ReadSnapshotState.SNAPSHOT, OptionalLong.of(1), "source-1", identity, Optional.empty());
        return new SplitSourceDescriptor(
                "lake", "raw", "items", Constraint.EMPTY, List.of("value"), Optional.of(binding));
    }

    private static IndexDescriptor descriptor() {
        var column = new ConnectorTableMetadata.Column("value", PangrovaType.BIGINT, OptionalInt.of(1), null);
        return new IndexDescriptor(
                "index-1", "items_value", "lake", "raw", "items", List.of("value"),
                IndexType.BLOOM_FILTER,
                Map.of(IndexDescriptor.SOURCE_SCHEMA_FINGERPRINT_PROPERTY,
                        IndexSchemaFingerprint.of(List.of("value"), List.of(column))),
                IndexState.READY, null, 0, "snapshot-1", "snapshot-1", NOW, NOW, "tenant");
    }
}
