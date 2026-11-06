package org.pangrova.batch.api.block;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.types.value.ColumnValue;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlockRegionBoundsTest {
    static Stream<Block> blocks() {
        var single = new IntBlock(new int[]{42}, new boolean[]{false}, 1);
        return Stream.of(
                new ArrayBlock(new ColumnValue[]{new ColumnValue.IntValue(42)}),
                single,
                new LongBlock(new long[]{42}, new boolean[]{false}, 1),
                new DoubleBlock(new double[]{42}, new boolean[]{false}, 1),
                new SliceBlock(new byte[][]{new byte[]{42}}, new boolean[]{false}, 1),
                new DictionaryBlock(single, new int[]{0}),
                new RunLengthEncodedBlock(single, 1),
                new LazyBlock(() -> single, 1));
    }

    @ParameterizedTest
    @MethodSource("blocks")
    void overflowedEndCannotAuthorizeRegion(Block block) {
        assertThatThrownBy(() -> block.getRegion(Integer.MAX_VALUE, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @ParameterizedTest
    @MethodSource("blocks")
    void validEmptyEndAndFullRegionRemainAvailable(Block block) {
        assertThat(block.getRegion(1, 0).getPositionCount()).isZero();
        assertThat(block.getRegion(0, 1).getPositionCount()).isEqualTo(1);
        assertThatThrownBy(() -> block.getRegion(2, 0)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> block.getRegion(-1, 1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> block.getRegion(0, -1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void maximumLengthEncodedBlockStillRejectsNonemptyRegionAtEnd() {
        var value = new IntBlock(new int[]{42}, new boolean[]{false}, 1);
        var block = new RunLengthEncodedBlock(value, Integer.MAX_VALUE);
        assertThat(block.getRegion(Integer.MAX_VALUE, 0).getPositionCount()).isZero();
        assertThatThrownBy(() -> block.getRegion(Integer.MAX_VALUE, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }
}
