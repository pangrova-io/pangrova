package org.pangrova.planner.analysis;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.SetQuery;
import org.pangrova.parser.syntax.TimeTravelSqlClauses;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.planner.sqlnode.PangrovaParserFactory;
import org.pangrova.planner.rewrite.DynamicTableSqlRewriter;
import org.pangrova.planner.rewrite.DynamicTableAstRewriter;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Ordinary SQL aliases must survive logical-to-physical Dynamic Table resolution. */
class DynamicTableParsedBindingTest {
    @Test
    void publicationRewritePreservesSetResultClauses() {
        var logical = new QualifiedTableName("lake", "analytics", "rollup");
        var physical = new QualifiedTableName("lake", "analytics", "__pg_mat_first");
        var resolver = new DynamicTableRewriter(parts -> parts.get(parts.size() - 1).equals(logical.table())
                ? Optional.of(new DynamicTableRewriter.PublishedTarget(logical, physical)) : Optional.empty());
        var before = (SetQuery) TimeTravelSqlClauses.extract(
                "SELECT id FROM lake.analytics.rollup UNION ALL SELECT 1 AS id ORDER BY id LIMIT 2 OFFSET 1")
                .statement();
        var rewrite = new DynamicTableAstRewriter(resolver).rewrite(before);
        var after = (SetQuery) rewrite.statement();

        assertThat(after.queries().get(0).from().get(0).name().parts())
                .containsExactly("lake", "analytics", "__pg_mat_first");
        assertThat(rewrite.physicalToLogical()).containsEntry(physical, logical);
        assertThat(after.orderBy()).isEqualTo(before.orderBy()).hasSize(1);
        assertThat(after.limit()).isEqualTo(before.limit()).isPresent();
        assertThat(after.offset()).isEqualTo(before.offset()).isPresent();
    }

    @Test
    void preservesSampleAndExactPhysicalIdentifierComponents() {
        var logical = new QualifiedTableName("lake", "analytics", "rollup");
        var physical = new QualifiedTableName("lake", "analytics.region", "__pg_mat first");
        var resolver = new DynamicTableRewriter(parts -> Optional.of(
                new DynamicTableRewriter.PublishedTarget(logical, physical)));
        var rewriter = new DynamicTableSqlRewriter(new PangrovaParserFactory(), resolver);
        var input = TimeTravelSqlClauses.extract("SELECT rollup.id FROM lake.analytics.rollup TABLESAMPLE SYSTEM (25)");
        var before = ((Query) input.statement()).from().get(0);
        var result = rewriter.rewriteWithAuthority(input);
        var after = ((Query) result.statement()).from().get(0);
        assertThat(after.name().parts()).containsExactly("lake", "analytics.region", "__pg_mat first");
        assertThat(after.alias()).contains("rollup");
        assertThat(after.tableSample()).isEqualTo(before.tableSample()).isPresent();
        assertThat(result.physicalToLogical()).containsEntry(physical, logical);
    }

    @Test
    void bindsPublishedPivotAndUnpivotWithTheirOriginalClauses() {
        var logical = new QualifiedTableName("lake", "analytics", "rollup");
        var physical = new QualifiedTableName("lake", "analytics", "__pg_mat_first");
        var root = CalciteSchema.createRootSchema(false);
        var schema = root.add("lake", new AbstractSchema()).add("analytics", new AbstractSchema());
        schema.add(physical.table(), new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory types) {
                return types.builder().add("amount", SqlTypeName.BIGINT)
                        .add("quarter", SqlTypeName.VARCHAR)
                        .add("revenue", SqlTypeName.BIGINT).add("cost", SqlTypeName.BIGINT).build();
            }
        });
        var resolver = new DynamicTableRewriter(parts -> Optional.of(
                new DynamicTableRewriter.PublishedTarget(logical, physical)));
        var rewriter = new DynamicTableSqlRewriter(new PangrovaParserFactory(), resolver);
        var planner = new PangrovaPlanner(root, List.of("lake", "analytics"),
                new PlannerContext(null, null, resolver, null));
        for (String sql : List.of(
                "SELECT * FROM lake.analytics.rollup PIVOT (SUM(amount) FOR quarter IN ('Q1')) AS p",
                "SELECT * FROM lake.analytics.rollup UNPIVOT (val FOR metric IN (revenue, cost)) AS u")) {
            var parsed = TimeTravelSqlClauses.extract(sql);
            var before = ((Query) parsed.statement()).from().get(0);
            var rewritten = rewriter.rewriteWithAuthority(parsed);
            var after = ((Query) rewritten.statement()).from().get(0);
            assertThat(after.name().parts()).containsExactly("lake", "analytics", "__pg_mat_first");
            assertThat(after.alias()).isEqualTo(before.alias());
            assertThat(after.pivotClause()).isEqualTo(before.pivotClause());
            assertThat(after.unpivotClause()).isEqualTo(before.unpivotClause());
            assertThat(rewritten.physicalToLogical()).containsEntry(physical, logical);
            assertThat(planner.planForPolicyApplication(parsed).toPlannedQuery().physicalToLogicalDynamicTables())
                    .containsEntry(physical, logical);
        }
    }

    @Test
    void bindsImplicitLogicalQualifierAndExplicitAlias() {
        var logical = new QualifiedTableName("lake", "analytics", "rollup");
        var physical = new QualifiedTableName("lake", "analytics", "__pg_mat_first");
        var root = CalciteSchema.createRootSchema(false);
        var schema = root.add("lake", new AbstractSchema()).add("analytics", new AbstractSchema());
        for (String name : List.of(physical.table(), "ordinary")) {
            schema.add(name, new AbstractTable() {
                @Override
                public RelDataType getRowType(RelDataTypeFactory types) {
                    return types.builder().add("id", SqlTypeName.BIGINT).build();
                }
            });
        }
        var rewriter = new DynamicTableRewriter(parts -> parts.get(parts.size() - 1).equals(logical.table())
                ? Optional.of(new DynamicTableRewriter.PublishedTarget(logical, physical)) : Optional.empty());
        var planner = new PangrovaPlanner(root, List.of("lake", "analytics"),
                new PlannerContext(null, null, rewriter, null));

        for (String sql : List.of("SELECT rollup.id FROM lake.analytics.rollup",
                "SELECT rollup.id FROM rollup", "SELECT r.id FROM lake.analytics.rollup r",
                "SELECT id FROM lake.analytics.rollup", "SELECT \"rollup\".id FROM lake.analytics.\"rollup\"")) {
            var result = planner.planForPolicyApplication(TimeTravelSqlClauses.extract(sql));
            assertThat(result.preOptimizationColumnNames()).containsExactly("id");
            assertThat(result.toPlannedQuery().physicalToLogicalDynamicTables()).containsEntry(physical, logical);
        }
        var ordinary = planner.planForPolicyApplication(
                TimeTravelSqlClauses.extract("SELECT ordinary.id FROM lake.analytics.ordinary"));
        assertThat(ordinary.preOptimizationColumnNames()).containsExactly("id");
        assertThat(ordinary.toPlannedQuery().physicalToLogicalDynamicTables()).isEmpty();
    }
}
