package org.pangrova.planner.mv;

import org.pangrova.parser.ast.RefreshStrategy;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
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
 * Correctness regression tests for architecture-remediation #119.
 *
 * <p>An MV defined WITH a WHERE clause materializes only the subset of rows
 * matching that predicate. The structural subsumption matcher must NOT rewrite
 * a query to read from such an MV unless the query's predicate provably implies
 * the MV's predicate — otherwise the rewritten plan returns rows the MV never
 * contained (wrong results). Until full predicate-containment is implemented,
 * any MV whose definition contains a WHERE (or HAVING) predicate must be
 * refused for rewrite.</p>
 */
class WhereDefinedMvSubsumptionTest {

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

    /**
     * Positive control: an MV with NO defining predicate still matches, so the
     * guard does not over-block legitimate rewrites.
     */
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

        Optional<SubsumptionResult> result = matcher.match(queryPlan, mvDef);

        assertThat(result).isPresent();
    }

    private RelNode scanWithColumns(String tableName, List<String> columnNames) {
        RelDataType rowType = buildRowType(columnNames);
        RelOptTable table = new MaterializedViewTableRef(tableName, rowType);
        return LogicalTableScan.create(cluster, table, List.of());
    }

    private RelDataType buildRowType(List<String> columnNames) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (String col : columnNames) {
            builder.add(col, SqlTypeName.VARCHAR);
        }
        return builder.build();
    }
}
