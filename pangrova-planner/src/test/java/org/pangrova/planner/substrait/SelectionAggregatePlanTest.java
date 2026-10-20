package org.pangrova.planner.substrait;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.PangrovaPlanner;

import static org.assertj.core.api.Assertions.assertThat;

class SelectionAggregatePlanTest {
    @ParameterizedTest
    @ValueSource(strings = {"MAX_BY", "MIN_BY", "ARG_MAX", "ARG_MIN"})
    void preservesTheEntireValueTypeAndUsesTheEngineExtension(String function) throws Exception {
        var schema = CalciteSchema.createRootSchema(false, false);
        schema.add("entries", new AbstractTable() {
            @Override public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("snapshot_id", SqlTypeName.BIGINT)
                        .add("spec_id", SqlTypeName.INTEGER).add("commit_time", SqlTypeName.BIGINT).build();
            }
        });
        var result = new PangrovaPlanner(schema).plan(
                "SELECT " + function + "(ROW(snapshot_id, spec_id), commit_time) AS chosen FROM entries");
        var chosen = result.plan().getRowType().getFieldList().get(0).getType();
        assertThat(chosen.getFieldList().stream().map(field -> field.getType().getSqlTypeName()))
                .containsExactly(SqlTypeName.BIGINT, SqlTypeName.INTEGER);
        var converted = new SubstraitConverter().convert(result.plan());
        var bytes = new SubstraitPlanSerializer().serialize(converted);
        var wire = io.substrait.proto.Plan.parseFrom(bytes);
        assertThat(wire.getExtensionUrnsList().stream().map(urn -> urn.getUrn()))
                .contains("extension:org.pangrova:functions_aggregate");
        assertThat(wire.getExtensionsList().stream().filter(extension -> extension.hasExtensionFunction())
                .map(extension -> extension.getExtensionFunction().getName()))
                .anyMatch(name -> name.startsWith(function.endsWith("MAX") || function.startsWith("MAX")
                        ? "max_by" : "min_by"));
    }
}
