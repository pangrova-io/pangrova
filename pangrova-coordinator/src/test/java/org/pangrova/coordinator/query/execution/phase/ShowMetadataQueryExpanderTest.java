package org.pangrova.coordinator.query.execution.phase;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.parser.SqlParser;
import org.pangrova.planner.MetadataTableQuery;
import org.pangrova.spi.connector.metadata.MetadataTableKind;
import org.pangrova.spi.connector.metadata.MetadataTableListing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShowMetadataQueryExpanderTest {
    private final SqlParser parser = new SqlParser();

    @Test
    void existingShowKindsResolveTheirNativeListingIntent() {
        for (var kind : List.of(MetadataTableKind.SNAPSHOTS, MetadataTableKind.FILES, MetadataTableKind.MANIFESTS)) {
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
    void unsupportedKindsFailWithoutDispatchingCoordinatorRows() {
        assertThatThrownBy(() -> ShowMetadataQueryExpander.expand(
                parser.parse("SHOW PARTITIONS FROM db.orders"), (parts, kind) -> {
                    throw new AssertionError("Unsupported reader must not resolve");
                })).isInstanceOf(PangrovaException.class).hasMessageContaining("no Native system-table reader");
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
        var session = new CoordinatorSessionContext("listing", Map.of(), null, "tenant");
        var query = new MetadataTableQuery(List.of("lake", "db", "orders"),
                new MetadataTableListing(MetadataTableKind.REFS, Map.of("type", "TAG")));
        var context = QueryExecutionContext.initial(new QueryStateMachine(), sql, session, null)
                .withMetadataQuery(query).withCompiler(null).withTimeTravelResult(sql, Map.of())
                .withCompiledPlan("plan", List.of("name")).withCacheKey("key").withStrippedSql(sql, "key");
        assertThat(context.metadataQuery()).isSameAs(query);
        assertThat(context.sql()).isEqualTo(sql);
        assertThat(context.originalSql()).isEqualTo(sql);
    }
}
