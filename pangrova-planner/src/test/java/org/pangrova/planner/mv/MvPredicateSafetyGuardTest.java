package org.pangrova.planner.mv;

import org.pangrova.parser.ast.RefreshStrategy;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests structural subsumption matching for materialized views.
 */
class MvPredicateSafetyGuardTest {

    private StructuralSubsumptionMatcher matcher;
    private RelOptCluster cluster;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    void setUp() {
        matcher = new StructuralSubsumptionMatcher();
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.setExecutor(RexUtil.EXECUTOR);
        cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    @Test
    void predicateFreeMv_stillMatches() {
        MaterializedViewDefinition mvDef = new MaterializedViewDefinition(
                "all_orders",
                "SELECT id, name, price FROM orders",
                RefreshStrategy.ON_SCHEDULE, true,
                Optional.of(Instant.now()), Instant.now(),
                Set.of("catalog.schema.orders"),
                Set.of("id", "name", "price"),
                Set.of());

        RelNode queryPlan = scanWithColumns("catalog.schema.orders",
                List.of("id", "name", "price"));

        assertThat(matcher.match(queryPlan, mvDef))
                .as("Predicate-free MV must still match")
                .isPresent();
    }

    @Test
    void whereInsideStringLiteral_doesNotBlock() {
        MaterializedViewDefinition mvDef = new MaterializedViewDefinition(
                "labeled_orders",
                "SELECT id, 'WHERE clause text' AS label FROM orders",
                RefreshStrategy.ON_SCHEDULE, true,
                Optional.of(Instant.now()), Instant.now(),
                Set.of("catalog.schema.orders"),
                Set.of("id", "label"),
                Set.of());

        RelNode queryPlan = scanWithColumns("catalog.schema.orders",
                List.of("id", "label"));

        assertThat(matcher.match(queryPlan, mvDef))
                .as("WHERE inside a string literal must not block rewrite")
                .isPresent();
    }

    private RelNode scanWithColumns(String tableName, List<String> columnNames) {
        RelDataType rowType = buildRowType(columnNames);
        return LogicalTableScan.create(cluster,
                new MaterializedViewTableRef(tableName, rowType), List.of());
    }

    private RelDataType buildRowType(List<String> columnNames) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (String col : columnNames) {
            builder.add(col, SqlTypeName.VARCHAR);
        }
        return builder.build();
    }
}
