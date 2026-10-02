package org.pangrova.protocol.substrait;

import io.substrait.proto.RelCommon;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Mapping laws include duplicates and zero fields rather than assuming a permutation. */
class SubstraitRelationLayoutPropertyTest {
    @Property(tries = 300)
    void emitCompositionIsEquivalentToTwoOrderedSelections(@ForAll("mappings") List<Integer> first,
                                                           @ForAll List<Integer> selectors) {
        List<Integer> second = first.isEmpty() ? List.of()
                : selectors.stream().map(value -> Math.floorMod(value, first.size())).toList();
        var composed = SubstraitRelationLayout.compose(emit(first), emit(second), 4);
        assertThat(composed.getEmit().getOutputMappingList()).isEqualTo(second.stream().map(first::get).toList());
    }

    @Property(tries = 300)
    void appendedInputPreservesOldOutputOrderAndShiftsOnlyAppendedExpressions(
            @ForAll("mappings") List<Integer> original, @ForAll boolean append) {
        var adjusted = SubstraitRelationLayout.withAppendedInput(emit(original), 2, 2, 1, append);
        var expected = new java.util.ArrayList<>(original.stream().map(index -> index < 2 ? index : index + 1).toList());
        if (append) expected.add(2);
        assertThat(adjusted.getEmit().getOutputMappingList()).isEqualTo(expected);
        assertThat(adjusted.hasEmit()).isTrue();
    }

    @Provide
    Arbitrary<List<Integer>> mappings() {
        return Arbitraries.integers().between(0, 3).list().ofMinSize(0).ofMaxSize(20);
    }

    private static RelCommon emit(List<Integer> mapping) {
        return RelCommon.newBuilder().setEmit(RelCommon.Emit.newBuilder().addAllOutputMapping(mapping)).build();
    }
}
