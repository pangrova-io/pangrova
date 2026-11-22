package org.pangrova.planner.cost;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.planner.analysis.PlannerContext;
import org.pangrova.planner.statistics.StatisticsCollector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PangrovaRelMdSelectivityTest {
    private final List<QualifiedTableName> reads = new ArrayList<>();
    private final StatisticsCollector statistics = new StatisticsCollector(table -> {
        reads.add(table);
        long ndv = switch (table.table()) {
            case "a" -> 2;
            case "b" -> 100;
            default -> 1_000;
        };
        return tableStats(ndv);
    });

    @ParameterizedTest
    @ValueSource(strings = {"b.k = c.k", "c.k = b.k"})
    void joinsReadTheOriginsOfBothKeysRegardlessOfOperandOrder(String condition) {
        Join join = firstInputOfType(plan("SELECT a.id FROM a JOIN b ON a.id=b.id JOIN c ON " + condition),
                Join.class);

        assertThat(join.getCluster().getMetadataQuery().getSelectivity(join, null)).isEqualTo(0.001);
        assertThat(reads).containsExactly(new QualifiedTableName("cat", "db", "b"),
                new QualifiedTableName("cat", "db", "c"));
    }

    @Test
    void derivedFilterColumnsDoNotReadBaseColumnStatistics() {
        Filter filter = firstInputOfType(plan("SELECT mapped FROM (SELECT k + 1 AS mapped FROM a) WHERE mapped = 5"),
                Filter.class);

        assertThat(filter.getCluster().getMetadataQuery().getSelectivity(filter, null))
                .isEqualTo(RelMdUtil.guessSelectivity(filter.getCondition()));
        assertThat(reads).isEmpty();
    }

    @Test
    void multipleEqualityKeysDoNotAssumeIndependentColumnDistributions() {
        Join join = firstInputOfType(plan("SELECT b.id FROM b JOIN c ON b.k = c.k AND b.k2 = c.k2"), Join.class);

        assertThat(join.getCluster().getMetadataQuery().getSelectivity(join, null)).isEqualTo(0.001);
    }

    @Test
    void unknownKeyNdvUsesCalciteEstimate() {
        Join join = firstInputOfType(plan("SELECT b.id FROM b JOIN c ON b.id = c.k"), Join.class);

        assertThat(join.getCluster().getMetadataQuery().getSelectivity(join, null))
                .isEqualTo(RelMdUtil.guessSelectivity(join.getCondition()));
    }

    @Test
    void sharedHandlersUseEachPlanningScopesOwnStatistics() {
        String sql = "SELECT b.id FROM b JOIN c ON b.k = c.k";
        Join first = firstInputOfType(plan(sql), Join.class);
        Join second = firstInputOfType(plan(sql, new StatisticsCollector(table -> tableStats(20))), Join.class);

        assertThat(first.getCluster().getMetadataProvider()).isSameAs(second.getCluster().getMetadataProvider());
        assertThat(first.getCluster().getMetadataQuery().getSelectivity(first, null)).isEqualTo(0.001);
        assertThat(second.getCluster().getMetadataQuery().getSelectivity(second, null)).isEqualTo(0.05);
    }

    private static TableStatistics tableStats(long ndv) {
        var column = new TableStatistics.ColumnStatistics(0, ndv, 1L, ndv, 8.0);
        return new TableStatistics(10_000, 80_000, 1, Map.of("k", column, "k2", column));
    }

    private RelNode plan(String sql) {
        return plan(sql, statistics);
    }

    private static RelNode plan(String sql, StatisticsCollector collector) {
        var root = CalciteSchema.createRootSchema(false);
        var database = root.add("cat", new AbstractSchema()).add("db", new AbstractSchema());
        for (String name : List.of("a", "b", "c")) {
            database.add(name, new AbstractTable() {
                @Override
                public RelDataType getRowType(RelDataTypeFactory types) {
                    return types.builder().add("id", SqlTypeName.BIGINT).add("k", SqlTypeName.BIGINT)
                            .add("k2", SqlTypeName.BIGINT).build();
                }
            });
        }
        var context = new PlannerContext(collector, null, null, null);
        return new PangrovaPlanner(root, List.of("cat", "db"), context).planForPolicyApplication(sql).plan();
    }

    private static <T extends RelNode> T firstInputOfType(RelNode node, Class<T> type) {
        RelNode input = node;
        while (!type.isInstance(input)) {
            input = input.getInput(0);
        }
        return type.cast(input);
    }
}
