package org.pangrova.planner.substrait;

import io.substrait.proto.CrossRel;
import io.substrait.proto.ExtensionSingleRel;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.SetRel;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for F-SUBSTRAIT-04 (task 122): patchCrossRels must recurse through
 * ExtensionSingleRel / ExtensionMultiRel / SetRel so that a CrossRel nested beneath
 * them still has its condition (field 4) and join type (field 5) injected.
 * Before the fix, the {@code default -> rel} branch returned these rel types
 * unchanged, so a nested CrossRel was never patched.
 */
class CrossRelNestedInjectionTest {

    private final SubstraitPlanSerializer serializer = new SubstraitPlanSerializer();

    // Raw wire bytes for CrossRel field 4 (expression, empty message) + field 5 (join type = 1).
    private static final byte[] EXTRA_FIELDS = {0x22, 0x00, 0x28, 0x01};

    private static Rel leafRead() {
        return Rel.newBuilder().setRead(ReadRel.newBuilder().build()).build();
    }

    private static Rel crossRel() {
        return Rel.newBuilder()
                .setCross(CrossRel.newBuilder()
                        .setLeft(leafRead())
                        .setRight(leafRead())
                        .build())
                .build();
    }

    private static io.substrait.proto.Plan planWithRootInput(Rel input) {
        return io.substrait.proto.Plan.newBuilder()
                .addRelations(PlanRel.newBuilder()
                        .setRoot(RelRoot.newBuilder().setInput(input).build())
                        .build())
                .build();
    }

    @Test
    void crossNestedUnderExtensionSingleGetsFieldsInjected() throws Exception {
        Rel extSingle = Rel.newBuilder()
                .setExtensionSingle(ExtensionSingleRel.newBuilder()
                        .setInput(crossRel())
                        .build())
                .build();

        byte[] out = serializer.rebuildPlanWithCrossRelFields(
                planWithRootInput(extSingle), List.of(EXTRA_FIELDS));

        io.substrait.proto.Plan parsed = io.substrait.proto.Plan.parseFrom(out);
        Rel rootInput = parsed.getRelations(0).getRoot().getInput();
        assertThat(rootInput.getRelTypeCase()).isEqualTo(Rel.RelTypeCase.EXTENSION_SINGLE);
        CrossRel nestedCross = rootInput.getExtensionSingle().getInput().getCross();
        assertThat(nestedCross.getUnknownFields().hasField(4)).isTrue();
        assertThat(nestedCross.getUnknownFields().hasField(5)).isTrue();
    }

    @Test
    void crossNestedUnderSetRelGetsFieldsInjected() throws Exception {
        Rel set = Rel.newBuilder()
                .setSet(SetRel.newBuilder()
                        .addInputs(leafRead())
                        .addInputs(crossRel())
                        .build())
                .build();

        byte[] out = serializer.rebuildPlanWithCrossRelFields(
                planWithRootInput(set), List.of(EXTRA_FIELDS));

        io.substrait.proto.Plan parsed = io.substrait.proto.Plan.parseFrom(out);
        Rel rootInput = parsed.getRelations(0).getRoot().getInput();
        assertThat(rootInput.getRelTypeCase()).isEqualTo(Rel.RelTypeCase.SET);
        CrossRel nestedCross = rootInput.getSet().getInputs(1).getCross();
        assertThat(nestedCross.getUnknownFields().hasField(4)).isTrue();
        assertThat(nestedCross.getUnknownFields().hasField(5)).isTrue();
    }
}
