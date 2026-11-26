package org.pangrova.planner.analysis;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.api.statistics.ColumnStatistics;
import org.pangrova.planner.statistics.PlannerTableStatistics;
import org.pangrova.planner.statistics.StatisticsCollector;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class StatisticsPruningSafetyTest {
    @ParameterizedTest
    @ValueSource(strings = {"amount = 999", "amount < 999", "amount <= 999", "amount > 0",
            "amount >= 0", "999 > amount", "0 < amount", "amount = 200", "amount IS NULL"})
    void staleCostStatisticsCannotReplaceAProtectedSourceWithAnEmptyRelation(String predicate) throws Exception {
        var statistics = new StatisticsCollector();
        var column = new ColumnStatistics(1000, 0, 100L, 500L, 100, 8, Optional.empty(), 1);
        statistics.putStatistics(new QualifiedTableName("test", "db", "orders"), new PlannerTableStatistics(
                new QualifiedTableName("test", "db", "orders"), Map.of("amount", column), Instant.EPOCH));
        var schema = CalciteSchema.createRootSchema(false);
        var database = schema.plus().add("test", new org.apache.calcite.schema.impl.AbstractSchema())
                .add("db", new org.apache.calcite.schema.impl.AbstractSchema());
        database.add("orders", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("amount", SqlTypeName.BIGINT).nullable(true).build();
            }
        });
        var context = new PlannerContext(statistics, null, null, null, null, null);
        var planner = new PangrovaPlanner(schema, List.of("test", "db"), context);
        var compiler = new SqlCompiler(planner, new SubstraitConverter(), new SubstraitPlanSerializer());
        String sql = "SELECT amount FROM orders WHERE " + predicate;
        assertThat(planner.plan(sql).plan().explain()).doesNotContain("condition=[false]");
        var plan = Plan.parseFrom(compiler.compile(sql));
        assertThat(source(plan.getRelations(0).getRoot().getInput()).getRead()
                .getNamedTable().getNamesList()).containsExactly("test", "db", "orders");
    }

    private static Rel source(Rel relation) {
        return switch (relation.getRelTypeCase()) {
            case READ -> relation;
            case FILTER -> source(relation.getFilter().getInput());
            case PROJECT -> source(relation.getProject().getInput());
            default -> throw new AssertionError("Source discarded by statistics: " + relation.getRelTypeCase());
        };
    }
}
