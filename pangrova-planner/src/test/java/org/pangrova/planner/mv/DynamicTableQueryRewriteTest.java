package org.pangrova.planner.mv;

import org.pangrova.planner.parse.PangrovaParserFactory;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.parse.TimeTravelSqlClauses;
import org.pangrova.planner.rewrite.DynamicTableSqlRewriter;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Dynamic Table query transparency rewrite over durable publications. */
class DynamicTableQueryRewriteTest {

    @Test
    void selfJoinPinsOnePublishedGenerationBeforeSecondReference() {
        var logical = new QualifiedTableName("lake", "analytics", "rollup");
        var physical = new QualifiedTableName("lake", "analytics", "__pg_mat_first");
        var reads = new AtomicInteger();
        DynamicTableRewriter.PublishedTargetResolver resolver =
                new DynamicTableRewriter.PublishedTargetResolver() {
                    @Override
                    public Optional<QualifiedTableName> canonicalLogical(List<String> nameParts) {
                        return Optional.of(logical);
                    }

                    @Override
                    public Optional<DynamicTableRewriter.PublishedTarget> resolve(List<String> nameParts) {
                        if (reads.incrementAndGet() != 1) {
                            throw new IllegalStateException("Publication changed during one SQL rewrite");
                        }
                        return Optional.of(new DynamicTableRewriter.PublishedTarget(logical, physical));
                    }
                };
        var sqlRewriter = new DynamicTableSqlRewriter(
                new PangrovaParserFactory(), new DynamicTableRewriter(resolver));

        var rewritten = sqlRewriter.rewriteWithAuthority("SELECT a.id FROM lake.analytics.rollup a "
                + "JOIN lake.analytics.rollup b ON a.id = b.id");

        assertThat(reads).hasValue(1);
        assertThat(rewritten.sql()).contains("FROM lake.analytics.__pg_mat_first a",
                "JOIN lake.analytics.__pg_mat_first b");
        assertThat(rewritten.physicalToLogical()).containsEntry(physical, logical);
    }

    @Test
    void parsedSelfJoinPinsOnePublishedGenerationAndKeepsItsAliases() {
        var logical = new QualifiedTableName("lake", "analytics", "rollup");
        var physical = new QualifiedTableName("lake", "analytics", "__pg_mat_first");
        var reads = new AtomicInteger();
        var resolver = new DynamicTableRewriter.PublishedTargetResolver() {
            @Override
            public Optional<QualifiedTableName> canonicalLogical(List<String> nameParts) {
                return Optional.of(logical);
            }

            @Override
            public Optional<DynamicTableRewriter.PublishedTarget> resolve(List<String> nameParts) {
                reads.incrementAndGet();
                return Optional.of(new DynamicTableRewriter.PublishedTarget(logical, physical));
            }
        };
        var rewriter = new DynamicTableSqlRewriter(new PangrovaParserFactory(), new DynamicTableRewriter(resolver));
        var input = TimeTravelSqlClauses.extract("SELECT a.id FROM lake.analytics.\"rollup\" a "
                + "JOIN lake.analytics.rollup b ON a.id = b.id");
        var result = rewriter.rewriteWithAuthority(input);

        assertThat(reads).hasValue(1);
        var query = (Query) result.statement();
        assertThat(query.from()).singleElement().satisfies(ref -> {
            assertThat(ref.name().parts()).containsExactly("lake", "analytics", "__pg_mat_first");
            assertThat(ref.alias()).contains("a");
        });
        assertThat(query.joins()).singleElement().satisfies(join -> {
            assertThat(join.right().name().parts()).containsExactly("lake", "analytics", "__pg_mat_first");
            assertThat(join.right().alias()).contains("b");
        });
        assertThat(result.physicalToLogical()).containsEntry(physical, logical);
    }

    @Test
    void unpublishedNamePassesThrough() {
        var sqlRewriter = new DynamicTableSqlRewriter(new PangrovaParserFactory(),
                new DynamicTableRewriter(parts -> Optional.empty()));

        var rewritten = sqlRewriter.rewriteWithAuthority(
                "SELECT id FROM lake.analytics.rollup");

        assertThat(rewritten.sql()).isEqualTo("SELECT id FROM lake.analytics.rollup");
        assertThat(rewritten.physicalToLogical()).isEmpty();
    }

    @Test
    void explicitPhysicalGenerationInSameSqlCannotInheritLogicalAuthority() {
        var logical = new QualifiedTableName("lake", "analytics", "rollup");
        var physical = new QualifiedTableName("lake", "analytics", "__pg_mat_first");
        var sqlRewriter = new DynamicTableSqlRewriter(new PangrovaParserFactory(),
                new DynamicTableRewriter(parts -> Optional.of(
                        new DynamicTableRewriter.PublishedTarget(logical, physical))));

        for (String sql : List.of(
                "SELECT a.id FROM lake.analytics.rollup a JOIN lake.analytics.__pg_mat_first b ON a.id=b.id",
                "SELECT a.id FROM lake.analytics.rollup a, lake.analytics.__pg_mat_first b",
                "WITH c AS (SELECT id FROM lake.analytics.__pg_mat_first) "
                        + "SELECT id FROM lake.analytics.rollup")) {
            assertThatThrownBy(() -> sqlRewriter.rewriteWithAuthority(sql))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("Direct access");
            assertThatThrownBy(() -> sqlRewriter.rewriteWithAuthority(TimeTravelSqlClauses.extract(sql)))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("Direct access");
        }
        assertThat(sqlRewriter.rewriteWithAuthority(
                "SELECT id AS __pg_mat_alias FROM lake.analytics.rollup").sql())
                .contains("lake.analytics.__pg_mat_first");
    }
}
