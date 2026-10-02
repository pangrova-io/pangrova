package org.pangrova.planner.substrait;

import io.substrait.relation.Project;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.PangrovaPlanner;

import static org.assertj.core.api.Assertions.assertThat;

class WindowProjectionContractTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT ID, LAG(ID, 1) OVER (ORDER BY ID) AS PREVIOUS_ID FROM T1",
            "SELECT ID, LEAD(ID, 1) OVER (ORDER BY ID) AS NEXT_ID FROM T1",
            "SELECT ID, NTILE(4) OVER (ORDER BY ID) AS BUCKET FROM T1",
            "SELECT ID, ROW_NUMBER() OVER (PARTITION BY NAME) AS RN FROM T1",
            "SELECT ID, ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY ID) AS RN FROM T1"
    })
    void windowProjectEmitsExactlyTheCalciteOutputColumns(String sql) {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("T1", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                        .add("ID", SqlTypeName.INTEGER)
                        .add("NAME", SqlTypeName.VARCHAR, 20)
                        .build();
            }
        });
        RelNode plan = new PangrovaPlanner(schema).plan(sql).plan();
        LogicalWindow window = findWindow(plan);
        assertThat(window).isNotNull();

        var converted = new PlanNodeConverter(new TypeMapper(), new FunctionMapper(),
                window.getCluster().getRexBuilder()).visit(window);

        assertThat(converted).isInstanceOf(Project.class);
        assertThat(((Project) converted).getExtension()).isEmpty();
        assertThat(converted.getRecordType().fields())
                .hasSize(window.getRowType().getFieldCount());
        assertThat(converted.getRecordType().fields())
                .containsExactlyElementsOf(window.getRowType().getFieldList().stream()
                        .map(field -> new TypeMapper().map(field.getType()))
                        .toList());
    }

    private static LogicalWindow findWindow(RelNode node) {
        if (node instanceof LogicalWindow window) {
            return window;
        }
        for (RelNode input : node.getInputs()) {
            LogicalWindow window = findWindow(input);
            if (window != null) {
                return window;
            }
        }
        return null;
    }
}
