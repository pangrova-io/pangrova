package org.pangrova.planner.substrait;

import io.substrait.plan.Plan;
import org.apache.calcite.rel.RelNode;
import org.pangrova.planner.PangrovaPlanner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for SubstraitPlanSerializer: serialize, round-trip, and invalid input.
 */
class SubstraitPlanSerializerTest {

    private SubstraitPlanSerializer serializer;
    private SubstraitConverter converter;
    private PangrovaPlanner planner;

    @BeforeEach
    void setUp() {
        serializer = new SubstraitPlanSerializer();
        converter = new SubstraitConverter();
        planner = new PangrovaPlanner();
    }

    @Test
    void serializeSelectOneShouldProduceNonEmptyBytes() {
        Plan plan = buildPlan("SELECT 1");
        byte[] bytes = serializer.serialize(plan);
        assertThat(bytes).isNotNull().isNotEmpty();
    }

    @Test
    void roundTripShouldPreservePlanStructure() {
        Plan original = buildPlan("SELECT 1");
        byte[] bytes = serializer.serialize(original);
        Plan restored = serializer.deserialize(bytes);

        assertThat(restored.getRoots()).hasSameSizeAs(original.getRoots());
        assertThat(restored.getRoots().get(0).getNames())
                .isEqualTo(original.getRoots().get(0).getNames());
    }

    @Test
    void deserializeInvalidBytesShouldThrow() {
        byte[] garbage = {0x00, 0x01, 0x02, (byte) 0xFF};
        assertThatThrownBy(() -> serializer.deserialize(garbage))
                .isInstanceOf(SubstraitConversionException.class);
    }

    private Plan buildPlan(String sql) {
        RelNode relNode = planner.plan(sql).plan();
        return converter.convert(relNode);
    }
}
