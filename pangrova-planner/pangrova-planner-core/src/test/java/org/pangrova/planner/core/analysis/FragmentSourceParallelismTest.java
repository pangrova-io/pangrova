package org.pangrova.planner.core.analysis;

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
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

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
    void selectedSourceUnitsKeepSmallCompressedFilesIndependentlySchedulable() {
        RelNode scan = boundScan(8_053_064L, 12_280_568L, 4);

        assertThat(FragmentSourceParallelism.resolve(scan, 256L * 1024 * 1024, 128, 4))
                .isEqualTo(4);
        assertThat(FragmentSourceParallelism.resolve(scan, 256L * 1024 * 1024, 2, 4))
                .isEqualTo(2);

        var aggregateBuilder = RelBuilder.create(Frameworks.newConfigBuilder()
                .defaultSchema(boundSchema(8_053_064L, 12_280_568L, 4)).build());
        RelNode aggregate = aggregateBuilder.scan("bound")
                .aggregate(aggregateBuilder.groupKey(), aggregateBuilder.countStar("rows"))
                .build();
        var fragmented = PlanFragmenter.fragment(aggregate, 0);
        int sourceStageId = fragmented.transfers().stream()
                .filter(transfer -> transfer.shuffleType() == ShuffleType.SINGLETON)
                .findFirst().orElseThrow().upstreamStageId();
        assertThat(fragmented.stages()).filteredOn(stage -> stage.stageId() == sourceStageId)
                .singleElement().satisfies(stage -> assertThat(stage.parallelism()).isEqualTo(4));
    }

    @Test
    void unknownAndEmptySourceUnitsPreserveMetadataParallelism() {
        assertThat(FragmentSourceParallelism.resolve(boundScan(100, 800, -1), 256L * 1024 * 1024, 8, 4))
                .isEqualTo(1);
        assertThat(FragmentSourceParallelism.resolve(boundScan(0, 0, 0), 256L * 1024 * 1024, 8, 4))
                .isEqualTo(1);
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

    private static RelNode boundScan(long rows, long bytes, long sourceUnits) {
        var builder = RelBuilder.create(Frameworks.newConfigBuilder()
                .defaultSchema(boundSchema(rows, bytes, sourceUnits)).build());
        return builder.scan("bound").build();
    }

    private static org.apache.calcite.schema.SchemaPlus boundSchema(
            long rows, long bytes, long sourceUnits) {
        var schema = Frameworks.createRootSchema(true);
        var metadata = new ConnectorTableMetadata(new SchemaTableName("db", "bound"),
                List.of("id"), List.of(), Map.of());
        var binding = new ConnectorReadBinding(new SourceHandle(),
                new ConnectorTableReadMetadata(metadata, ConnectorTableReadMetadata.DeclarationEvidence.EXACT),
                new TableStatistics(rows, bytes, sourceUnits, Map.of()),
                rows == 0 ? ConnectorReadBinding.SnapshotState.EMPTY : ConnectorReadBinding.SnapshotState.SNAPSHOT,
                rows == 0 ? OptionalLong.empty() : OptionalLong.of(1), "selected", java.util.Optional.empty());
        schema.add("bound", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.BIGINT).build();
            }

            @Override
            public org.apache.calcite.schema.Statistic getStatistic() {
                return Statistics.of((double) rows, List.of());
            }

            @Override
            public <T> T unwrap(Class<T> type) {
                if (type.isInstance(binding)) return type.cast(binding);
                return super.unwrap(type);
            }
        });
        return schema;
    }

    private record SourceHandle() implements ConnectorTableHandle {}
}
