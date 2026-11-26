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
import org.pangrova.parser.parse.TimeTravelSqlClauses;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.pangrova.planner.rewrite.DynamicTableSqlRewriter;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Ordinary SQL aliases must survive logical-to-physical Dynamic Table resolution. */
class DynamicTableParsedBindingTest {
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
