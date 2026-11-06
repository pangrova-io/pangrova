package org.pangrova.planner.core.substrait;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompiledScanSchemasTest {

    @Test
    void preservesExactSchemaBytesForEveryNestedSource() {
        var read = source("id");
        var plan = Plan.newBuilder().addRelations(root(read)).addRelations(root(read)).build();
        assertThat(CompiledScanSchemas.extract(plan.toByteArray()))
                .containsOnlyKeys("catalog.schema.orders")
                .hasEntrySatisfying("catalog.schema.orders",
                        bytes -> assertThat(bytes).isEqualTo(read.getRead().getBaseSchema().toByteArray()));
    }

    @Test
    void refusesAnInconsistentRepeatedSourceRatherThanPickingASchema() {
        var plan = Plan.newBuilder().addRelations(root(source("id")))
                .addRelations(root(source("other"))).build();
        assertThatThrownBy(() -> CompiledScanSchemas.extract(plan.toByteArray()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("inconsistent scan schemas");
    }

    @Test
    void refusesMalformedWireBytes() {
        assertThatThrownBy(() -> CompiledScanSchemas.extract(new byte[] {(byte) 0x80}))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("canonical Substrait");
    }

    private static Rel source(String column) {
        return Rel.newBuilder().setRead(ReadRel.newBuilder()
                .setNamedTable(ReadRel.NamedTable.newBuilder()
                        .addNames("catalog").addNames("schema").addNames("orders"))
                .setBaseSchema(NamedStruct.newBuilder().addNames(column))).build();
    }

    private static PlanRel root(Rel relation) {
        return PlanRel.newBuilder().setRoot(RelRoot.newBuilder().setInput(relation)).build();
    }
}
