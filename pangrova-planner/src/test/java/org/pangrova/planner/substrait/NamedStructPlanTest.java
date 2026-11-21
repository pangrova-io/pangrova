package org.pangrova.planner.substrait;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.analysis.PangrovaPlanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NamedStructPlanTest {
    private PangrovaPlanner planner() {
        var root = CalciteSchema.createRootSchema(false, false);
        root.add("source", new AbstractTable() {
            @Override public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.BIGINT).nullable(true)
                        .add("spec", SqlTypeName.INTEGER).build();
            }
        });
        return new PangrovaPlanner(root);
    }

    @Test void namedFieldsRemainAddressableAfterWholeRowAggregation() throws Exception {
        var plan = planner().plan("SELECT g.chosen.spec_id, g.chosen.snapshot_id FROM "
                + "(SELECT MAX_BY(ROW(spec_id => spec, snapshot_id => id), id) AS chosen FROM source) g");
        assertThat(plan.plan().getRowType().getFieldNames()).containsExactly("spec_id", "snapshot_id");
        assertThat(new SubstraitPlanSerializer().serialize(new SubstraitConverter().convert(plan.plan())))
                .isNotEmpty();
    }

    @Test void mixedFieldsKeepPositionalNamesAndNullability() {
        var plan = planner().plan("SELECT STRUCT(named => id, spec) AS value FROM source");
        var type = plan.plan().getRowType().getFieldList().get(0).getType();
        assertThat(type.getFieldNames()).containsExactly("named", "EXPR$1");
        assertThat(type.getFieldList().get(0).getType().isNullable()).isTrue();
    }

    @Test void quotedNamesAndEscapesRemainAddressable() {
        var plan = planner().plan("SELECT r.value.\"Spec ID\", r.value.\"snap\"\"id\" FROM "
                + "(SELECT ROW(\"Spec ID\" => spec, \"snap\"\"id\" => id) AS value FROM source) r");
        assertThat(plan.plan().getRowType().getFieldNames()).containsExactly("Spec ID", "snap\"id");
    }

    @Test void simpleAndSearchedCaseUseBooleanConditionsInCalcite() throws Exception {
        for (String expression : java.util.List.of("CASE spec WHEN 1 THEN id ELSE 0 END",
                "CASE WHEN spec = 1 THEN id ELSE 0 END", "CASE id WHEN NULL THEN 1 ELSE 2 END")) {
            var plan = planner().plan("SELECT " + expression + " AS value FROM source GROUP BY " + expression);
            assertThat(new SubstraitPlanSerializer().serialize(new SubstraitConverter().convert(plan.plan())))
                    .isNotEmpty();
        }
    }

    @Test void duplicateDeclaredFieldsAreRejected() {
        assertThatThrownBy(() -> planner().plan("SELECT ROW(same => id, SAME => spec) FROM source"))
                .hasMessageContaining("Duplicate struct field name");
    }
}
