package org.pangrova.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure relational metadata tests; no connector or execution collaborator is substituted. */
class FragmentSourceParallelismTest {

    @Test
    void unknownSourceUsesBoundedUnknownParallelismRatherThanCalciteDefaultRows() {
        var schema = Frameworks.createRootSchema(true);
        schema.add("declared", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.BIGINT).build();
            }
        });
        var builder = RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(schema).build());
        RelNode scan = builder.scan("declared").build();

        assertThat(scan.getCluster().getMetadataQuery().getRowCount(scan)).isEqualTo(100.0);
        assertThat(FragmentSourceParallelism.resolve(scan, 1, 2, 4)).isEqualTo(2);
        assertThat(FragmentSourceParallelism.resolve(scan, 1, 8, 4)).isEqualTo(4);
    }

    @Test
    void observedLiteralRowsUseActualMetadataAndCeilingAtPartitionBoundary() {
        RelNode values = values(3);
        var metadata = values.getCluster().getMetadataQuery();
        assertThat(metadata.getRowCount(values)).isEqualTo(3.0);
        assertThat(metadata.getAverageRowSize(values)).isEqualTo(8.0);

        assertThat(FragmentSourceParallelism.resolve(values, 24, 8, 4)).isEqualTo(1);
        assertThat(FragmentSourceParallelism.resolve(values, 23, 8, 4)).isEqualTo(2);
        assertThat(FragmentSourceParallelism.resolve(values, 8, 8, 4)).isEqualTo(3);
    }

    @Test
    void observedLiteralRowsNeverExceedConfiguredMaximum() {
        assertThat(FragmentSourceParallelism.resolve(values(3), 1, 2, 4)).isEqualTo(2);
        assertThat(FragmentSourceParallelism.resolve(values(3), 1, 1, 4)).isEqualTo(1);
    }

    @Test
    void emptyLiteralInputKeepsNoRowsDespiteCalciteCostingFloor() {
        LogicalValues values = values(0);
        // RelMetadataQuery floors estimates below one; tuples remain the actual immutable source.
        assertThat(values.getTuples()).isEmpty();
        assertThat(values.estimateRowCount(values.getCluster().getMetadataQuery())).isZero();
        assertThat(values.getCluster().getMetadataQuery().getRowCount(values)).isEqualTo(1.0);
        assertThat(FragmentSourceParallelism.resolve(values, 8, 8, 4)).isEqualTo(1);
        assertThat(FragmentSourceParallelism.resolve(values, 8, 2, 4)).isEqualTo(1);
        var fragment = PlanFragmenter.fragment(values, 8);
        assertThat(fragment.stages()).singleElement().satisfies(stage -> {
            assertThat(stage.parallelism()).isEqualTo(1);
            assertThat(((LogicalValues) stage.fragment().relNode()).getTuples()).isEmpty();
        });
    }

    private static LogicalValues values(int rows) {
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(types));
        RelDataType rowType = types.builder().add("id", SqlTypeName.BIGINT).build();
        var tuples = ImmutableList.<ImmutableList<RexLiteral>>builder();
        for (int index = 0; index < rows; index++) {
            tuples.add(ImmutableList.of(cluster.getRexBuilder().makeExactLiteral(
                    BigDecimal.valueOf(index), rowType.getFieldList().get(0).getType())));
        }
        return LogicalValues.create(cluster, rowType, tuples.build());
    }
}
