package org.pangrova.coordinator.query.execution;

import org.junit.jupiter.api.Test;
import org.pangrova.parser.parse.TimeTravelSqlClauses;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.indexing.IndexAwareSplitFetcher;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.rerank.RerankerPlanNode;
import org.pangrova.planner.exception.RerankerValidationException;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.descriptor.IndexSchemaFingerprint;
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
import org.pangrova.planner.analysis.SplitSourceDescriptor;
import org.pangrova.types.enums.PangrovaType;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DispatchContextEnricherTest {

    private static final String BM25_SQL = """
            SELECT content FROM catalog.schema.docs
            WHERE MATCH(content, 'distributed search')
            ORDER BY BM25_SCORE(content, 'distributed search') DESC
            LIMIT 10
            """;

    @Test
    void bindsReadyNativeBm25IndexAtItsPublishedSnapshot() {
        DispatchContextEnricher enricher = enricher(metadataStore(List.of(
                descriptor("standard-v1"))));

        DispatchQueryContext enriched = enricher.enrichWithReranker(
                sources(), context(BM25_SQL), TimeTravelSqlClauses.extract(BM25_SQL).statement(), "query-1", "tenant-a");

        RerankerPlanNode plan = enriched.rerankerPlan().orElseThrow();
        var source = (RerankerPlanNode.FullTextSource) plan.sources().get(0);
        assertThat(source.scoringNode().indexId()).isEqualTo("text-index-1");
        assertThat(source.scoringNode().indexGeneration()).isEqualTo(11);
        assertThat(source.scoringNode().indexSnapshot()).isEqualTo("snapshot-11");
    }

    @Test
    void rejectsBm25QueryWithoutCompatibleReadyIndex() {
        DispatchContextEnricher enricher = enricher(metadataStore(List.of(
                descriptor("legacy-analyzer"))));

        assertThatThrownBy(() -> enricher.enrichWithReranker(
                sources(), context(BM25_SQL), TimeTravelSqlClauses.extract(BM25_SQL).statement(), "query-2", "tenant-a"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE))
                .hasMessageContaining("READY Native BM25 index");
    }

    @Test
    void propagatesMetadataFailureInsteadOfSelectingSilentFallback() {
        IndexMetadataStore store = metadataStore(List.of());
        IndexMetadataStore failing = (IndexMetadataStore) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {IndexMetadataStore.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("findIndexesByTable")) {
                        throw new IllegalStateException("metastore unavailable");
                    }
                    return method.invoke(store, arguments);
                });

        assertThatThrownBy(() -> enricher(failing).enrichWithReranker(
                sources(), context(BM25_SQL), TimeTravelSqlClauses.extract(BM25_SQL).statement(), "query-3", "tenant-a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("metastore unavailable");
    }

    @Test
    void propagatesInvalidBm25SemanticsInsteadOfDroppingReranker() {
        String sql = """
                SELECT content FROM catalog.schema.docs
                ORDER BY BM25_SCORE(content, 'distributed search') DESC
                LIMIT 10
                """;

        assertThatThrownBy(() -> enricher(metadataStore(List.of(descriptor("standard-v1"))))
                .enrichWithReranker(sources(), context(sql), TimeTravelSqlClauses.extract(sql).statement(),
                        "query-4", "tenant-a"))
                .isInstanceOf(RerankerValidationException.class)
                .hasMessageContaining("positive MATCH");
    }

    private static DispatchContextEnricher enricher(IndexMetadataStore store) {
        return new DispatchContextEnricher(new IndexAwareSplitFetcher(
                null, null, store, null));
    }

    private static DispatchQueryContext context(String sql) {
        return new DispatchQueryContext(Map.of(), sql, new SqlCompiler());
    }

    private static IndexDescriptor descriptor(String analyzer) {
        Instant now = Instant.parse("2026-09-12T00:00:00Z");
        return new IndexDescriptor(
                "text-index-1", "docs_content_bm25", "catalog", "schema", "docs",
                List.of("content"), IndexType.INVERTED,
                Map.of("algorithm", "BM25", "analyzer", analyzer,
                        IndexDescriptor.SOURCE_SCHEMA_FINGERPRINT_PROPERTY,
                        IndexSchemaFingerprint.of(List.of("content"), List.of(content()))),
                IndexState.READY, null, 11, "snapshot-11", "snapshot-11",
                now, now, "tenant-1");
    }

    private static List<SplitSourceDescriptor> sources() {
        var column = content();
        var metadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "docs"), List.of("content"), List.of(column), Map.of());
        var identity = new ConnectorReadSchemaIdentity(
                new TableIdentity("TEST", "table-uuid", "file:///docs", "schema-1"),
                "test-field-namespace", List.of(column), FieldIdStability.SELECTED_SOURCE);
        var declaration = new ConnectorTableReadMetadata(metadata, TableDeclarationEvidence.EXACT);
        var binding = new ConnectorReadBinding(
                new ConnectorTableHandle() {}, declaration, TableStatistics.UNKNOWN,
                ReadSnapshotState.SNAPSHOT, OptionalLong.of(11), "source-1",
                Optional.of(identity), Optional.empty());
        return List.of(new SplitSourceDescriptor(
                "catalog", "schema", "docs", Constraint.EMPTY,
                List.of("content"), Optional.of(binding)));
    }

    private static ConnectorTableMetadata.Column content() {
        return new ConnectorTableMetadata.Column("content", PangrovaType.VARCHAR, OptionalInt.of(1), null);
    }

    private static IndexMetadataStore metadataStore(List<IndexDescriptor> descriptors) {
        return (IndexMetadataStore) Proxy.newProxyInstance(
                DispatchContextEnricherTest.class.getClassLoader(),
                new Class<?>[] {IndexMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "findIndexesByTable" -> descriptors;
                    case "toString" -> "IndexMetadataStoreTestDouble";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new AssertionError(
                            "Unexpected IndexMetadataStore call: " + method.getName());
                });
    }
}
