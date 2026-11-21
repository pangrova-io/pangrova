package org.pangrova.connector.runtime.scan;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.ConnectorSplit;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixedSplitSourceTest {
    private record Split(int id) implements ConnectorSplit {}

    @Test
    void batchesPreserveOrderBoundAndSnapshotWithoutLosingRemainder() {
        var input = new ArrayList<ConnectorSplit>(List.of(new Split(1), new Split(2), new Split(3)));
        var source = new FixedSplitSource(input);
        input.clear();
        assertThat(source.getNextBatch(2)).containsExactly(new Split(1), new Split(2));
        assertThat(source.isFinished()).isFalse();
        assertThat(source.getNextBatch(Integer.MAX_VALUE)).containsExactly(new Split(3));
        assertThat(source.isFinished()).isTrue();
        assertThat(source.getNextBatch(1)).isEmpty();
    }

    @Test
    void invalidBatchDoesNotConsumeAndCloseDiscardsRemainingSplits() {
        var source = new FixedSplitSource(List.of(new Split(1), new Split(2)));
        assertThatThrownBy(() -> source.getNextBatch(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> source.getNextBatch(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThat(source.getNextBatch(1)).containsExactly(new Split(1));
        source.close();
        source.close();
        assertThat(source.isFinished()).isTrue();
        assertThat(source.getNextBatch(1)).isEmpty();
        assertThat(new FixedSplitSource(List.of()).isFinished()).isTrue();
    }
}
