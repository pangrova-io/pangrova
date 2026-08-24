package org.pangrova.planner.mv;

import org.pangrova.parser.ast.RefreshStrategy;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for StructuralSubsumptionMatcher verifying projection, filter,
 * and rollup subsumption matching.
 */
class StructuralSubsumptionMatcherTest {

    private StructuralSubsumptionMatcher matcher;
    private RelOptCluster cluster;
    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;

    @BeforeEach
    void setUp() {
        matcher = new StructuralSubsumptionMatcher();
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.setExecutor(RexUtil.EXECUTOR);
        rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    @Test
    void projectionSubsumption_queryUsesSubsetOfColumns() {
        // MV has columns: id, name, price, quantity
        MaterializedViewDefinition mvDef = mvWithColumns(
                Set.of("catalog.schema.orders"),
                Set.of("id", "name", "price", "quantity"));

        // Query scans orders table with columns id, name, price, quantity
        RelNode queryPlan = scanWithColumns("catalog.schema.orders",
                List.of("id", "name", "price", "quantity"));

        Optional<SubsumptionResult> result = matcher.match(queryPlan, mvDef);

        assertThat(result).isPresent();
    }

    @Test
    void projectionSubsumption_queryNeedsColumnNotInMv() {
        // MV has columns: id, name
        MaterializedViewDefinition mvDef = mvWithColumns(
                Set.of("catalog.schema.orders"),
                Set.of("id", "name"));

        // Query needs id, name, price — price not in MV
        RelNode queryPlan = scanWithColumns("catalog.schema.orders",
                List.of("id", "name", "price"));

        Optional<SubsumptionResult> result = matcher.match(queryPlan, mvDef);

        assertThat(result).isEmpty();
    }

    @Test
    void filterSubsumption_queryAddsFilterOnMvColumns() {
        // MV has columns: id, name, price
        MaterializedViewDefinition mvDef = mvWithColumns(
                Set.of("catalog.schema.orders"),
                Set.of("id", "name", "price"));

        // Query: SELECT * FROM orders WHERE price > '100'
        RelNode scan = scanWithColumns("catalog.schema.orders",
                List.of("id", "name", "price"));
        RexNode filterCondition = rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2),
                rexBuilder.makeLiteral("100"));
        RelNode filteredPlan = LogicalFilter.create(scan, filterCondition);

        Optional<SubsumptionResult> result = matcher.match(filteredPlan, mvDef);

        assertThat(result).isPresent();
        assertThat(result.get().needsFilter()).isTrue();
    }

    @Test
    void filterSubsumption_queryFiltersOnColumnNotInMv() {
        // MV has columns: id, name
        MaterializedViewDefinition mvDef = mvWithColumns(
                Set.of("catalog.schema.orders"),
                Set.of("id", "name"));

        // Query: SELECT * FROM orders WHERE price > '100' (price not in MV)
        RelNode scan = scanWithColumns("catalog.schema.orders",
                List.of("id", "name", "price"));
        RexNode filterCondition = rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2),
                rexBuilder.makeLiteral("100"));
        RelNode filteredPlan = LogicalFilter.create(scan, filterCondition);

        Optional<SubsumptionResult> result = matcher.match(filteredPlan, mvDef);

        assertThat(result).isEmpty();
    }

    @Test
    void rollupSubsumption_queryGroupBySubsetOfMvGroupBy() {
        // MV groups by: region, category (and has columns region, category, total)
        MaterializedViewDefinition mvDef = new MaterializedViewDefinition(
                "sales_by_region_category",
                "SELECT region, category, SUM(amount) as total FROM sales GROUP BY region, category",
                RefreshStrategy.ON_SCHEDULE, true,
                Optional.of(Instant.now()), Instant.now(),
                Set.of("catalog.schema.sales"),
                Set.of("region", "category", "total"),
                Set.of("region", "category"));

        // Query: SELECT region, SUM(total) FROM mv GROUP BY region
        // (groups by region only — subset of MV's GROUP BY)
        RelNode scan = scanWithColumns("catalog.schema.sales",
                List.of("region", "category", "total"));
        RelNode aggregate = LogicalAggregate.create(
                scan,
                List.of(),
                ImmutableBitSet.of(0),  // GROUP BY region (index 0)
                null,
                List.of());

        Optional<SubsumptionResult> result = matcher.match(aggregate, mvDef);

        assertThat(result).isPresent();
        assertThat(result.get().needsRollup()).isTrue();
        assertThat(result.get().rollupColumns()).containsExactly("region");
    }

    @Test
    void rollupSubsumption_queryGroupByNotSubsetOfMvGroupBy() {
        // MV groups by: region
        MaterializedViewDefinition mvDef = new MaterializedViewDefinition(
                "sales_by_region",
                "SELECT region, SUM(amount) as total FROM sales GROUP BY region",
                RefreshStrategy.ON_SCHEDULE, true,
                Optional.of(Instant.now()), Instant.now(),
                Set.of("catalog.schema.sales"),
                Set.of("region", "total"),
                Set.of("region"));

        // Query groups by category — not in MV GROUP BY
        RelNode scan = scanWithColumns("catalog.schema.sales",
                List.of("region", "category", "total"));
        RelNode aggregate = LogicalAggregate.create(
                scan,
                List.of(),
                ImmutableBitSet.of(1),  // GROUP BY category (index 1)
                null,
                List.of());

        Optional<SubsumptionResult> result = matcher.match(aggregate, mvDef);

        assertThat(result).isEmpty();
    }

    @Test
    void tableSubsumption_queryReferencesTableNotInMv() {
        // MV covers only orders table
        MaterializedViewDefinition mvDef = mvWithColumns(
                Set.of("catalog.schema.orders"),
                Set.of("id", "name"));

        // Query scans a different table
        RelNode queryPlan = scanWithColumns("catalog.schema.customers",
                List.of("id", "name"));

        Optional<SubsumptionResult> result = matcher.match(queryPlan, mvDef);

        assertThat(result).isEmpty();
    }

    @Test
    void noStructuralMetadata_returnsEmpty() {
        // MV without structural metadata
        MaterializedViewDefinition mvDef = new MaterializedViewDefinition(
                "no_metadata_mv", "SELECT 1",
                RefreshStrategy.ON_SCHEDULE, true,
                Optional.of(Instant.now()), Instant.now());

        RelNode queryPlan = scanWithColumns("catalog.schema.orders",
                List.of("id", "name"));

        Optional<SubsumptionResult> result = matcher.match(queryPlan, mvDef);

        assertThat(result).isEmpty();
    }

    @Test
    void directScan_noCompensatingOperationsNeeded() {
        // MV has exactly the columns the query needs
        MaterializedViewDefinition mvDef = mvWithColumns(
                Set.of("catalog.schema.orders"),
                Set.of("id", "name"));

        RelNode queryPlan = scanWithColumns("catalog.schema.orders",
                List.of("id", "name"));

        Optional<SubsumptionResult> result = matcher.match(queryPlan, mvDef);

        assertThat(result).isPresent();
        assertThat(result.get().isExactMatch()).isTrue();
    }

    // --- helpers ---

    private MaterializedViewDefinition mvWithColumns(Set<String> tables, Set<String> columns) {
        return new MaterializedViewDefinition(
                "test_mv",
                "SELECT ...",
                RefreshStrategy.ON_SCHEDULE, true,
                Optional.of(Instant.now()), Instant.now(),
                tables, columns, Set.of());
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
