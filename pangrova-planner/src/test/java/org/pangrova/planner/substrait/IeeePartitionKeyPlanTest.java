package org.pangrova.planner.substrait;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.analysis.PangrovaPlanner;

import static org.assertj.core.api.Assertions.assertThat;

class IeeePartitionKeyPlanTest {
    @Test void registeredIeeeKeysKeepTheirWidthAndNullableResult() throws Exception {
        var schema = CalciteSchema.createRootSchema(false, false);
        schema.add("source", new AbstractTable() {
            @Override public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("f32", SqlTypeName.REAL).nullable(true)
                        .add("f64", SqlTypeName.DOUBLE).nullable(true).build();
            }
        });
        var result = new PangrovaPlanner(schema).plan(
                "SELECT TO_IEEE754_32(f32) AS k32, TO_IEEE754_64(f64) AS k64 FROM source");
        var fields = result.plan().getRowType().getFieldList();
        assertThat(fields).allSatisfy(field -> {
            assertThat(field.getType().getSqlTypeName()).isEqualTo(SqlTypeName.VARBINARY);
            assertThat(field.getType().isNullable()).isTrue();
        });
        assertThat(fields.get(0).getType().getPrecision()).isEqualTo(4);
        assertThat(fields.get(1).getType().getPrecision()).isEqualTo(8);
        var wire = io.substrait.proto.Plan.parseFrom(new SubstraitPlanSerializer().serialize(
                new SubstraitConverter().convert(result.plan())));
        assertThat(wire.getExtensionUrnsList().stream().map(urn -> urn.getUrn()))
                .contains("extension:org.pangrova:functions_binary");
    }
}
