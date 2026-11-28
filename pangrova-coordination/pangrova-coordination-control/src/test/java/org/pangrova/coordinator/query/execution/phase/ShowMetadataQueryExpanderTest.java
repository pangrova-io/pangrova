package org.pangrova.coordinator.query.execution.phase;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.parser.parse.AntlrSqlParser;
import org.pangrova.planner.analysis.MetadataTableQuery;
import org.pangrova.connector.api.enums.MetadataTableKind;
import org.pangrova.connector.api.metadata.MetadataTableListing;

import static org.assertj.core.api.Assertions.assertThat;

class ShowMetadataQueryExpanderTest {
    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void existingShowKindsResolveTheirNativeListingIntent() {
        for (var kind : List.of(MetadataTableKind.SNAPSHOTS, MetadataTableKind.FILES, MetadataTableKind.MANIFESTS,
                MetadataTableKind.PARTITIONS)) {
            var query = ShowMetadataQueryExpander.expand(parser.parse("SHOW " + kind + " FROM lake.db.orders"),
                    (parts, requested) -> {
                        assertThat(requested).isEqualTo(kind);
                        return new MetadataTableQuery(parts, new MetadataTableListing(requested));
                    });
            assertThat(query.baseTable()).containsExactly("lake", "db", "orders");
        }
    }

    @Test
    void referenceKindsUseConnectorMappingWithoutChangingTheName() {
        for (var kind : List.of(MetadataTableKind.TAGS, MetadataTableKind.BRANCHES)) {
            var query = ShowMetadataQueryExpander.expand(
                    parser.parse("SHOW " + kind + " FROM lake.db.\"Orders.中文\"\"; select\""),
                    (parts, requested) -> {
                        assertThat(requested).isEqualTo(kind);
                        return new MetadataTableQuery(parts, new MetadataTableListing(
                                MetadataTableKind.REFS,
                                Map.of("type", kind == MetadataTableKind.TAGS ? "TAG" : "BRANCH")));
                    });
            assertThat(query.baseTable()).containsExactly("lake", "db", "Orders.中文\"; select");
            assertThat(query.listing().source()).isEqualTo(MetadataTableKind.REFS);
        }
    }

    @Test
    void ordinaryQueriesDoNotAcquireMetadataSemantics() {
        assertThat(ShowMetadataQueryExpander.expand(parser.parse("SELECT 1"), (parts, kind) -> {
            throw new AssertionError("Ordinary query must not resolve a listing");
        })).isNull();
    }

    @Test
    void contextCopiesPreserveTheTypedReadAndAuditSql() {
        String sql = "SHOW TAGS FROM lake.db.orders";
        var session = new QuerySession("listing", Map.of(), null, "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        var query = new MetadataTableQuery(List.of("lake", "db", "orders"),
                new MetadataTableListing(MetadataTableKind.REFS, Map.of("type", "TAG")));
        var compiler = new org.pangrova.planner.analysis.SqlCompiler();
        var plan = compiler.compileToPlannedQuery("SELECT 1 AS name");
        var context = QueryExecutionContext.initial(new QueryStateMachine(), sql, session, compiler)
                .withMetadataQuery(query).withCompilation(compiler, plan, List.of("name"), null, Map.of());
        assertThat(context.metadataQuery()).isSameAs(query);
        assertThat(context.sql()).isEqualTo(sql);
        assertThat(context.originalSql()).isEqualTo(sql);
    }
}
