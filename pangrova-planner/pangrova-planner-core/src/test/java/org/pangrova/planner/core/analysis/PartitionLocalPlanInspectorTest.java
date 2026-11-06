package org.pangrova.planner.core.analysis;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionLocalPlanInspectorTest {

    @ParameterizedTest
    @ValueSource(strings = {"SELECT id FROM orders", "SELECT id + 1 FROM orders WHERE id > 10"})
    void acceptsOnlyDeterministicStreamingMaps(String sql) {
        assertThat(PartitionLocalPlanInspector.isIndependent(planner().plan(sql).plan())).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT COUNT(*) FROM orders",
            "SELECT id FROM orders ORDER BY id LIMIT 1",
            "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id",
            "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM orders",
            "SELECT RAND() FROM orders",
            "SELECT id FROM orders UNION ALL SELECT id FROM orders",
            "SELECT 1"
    })
    void refusesOperationsRequiringCrossPartitionSemantics(String sql) {
        assertThat(PartitionLocalPlanInspector.isIndependent(planner().plan(sql).plan())).isFalse();
    }

    private static PangrovaPlanner planner() {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("orders", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.BIGINT).build();
            }
        });
        return new PangrovaPlanner(schema);
    }
}
