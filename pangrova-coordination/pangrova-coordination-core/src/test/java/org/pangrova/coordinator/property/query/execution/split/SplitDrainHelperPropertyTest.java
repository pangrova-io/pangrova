package org.pangrova.coordinator.property.query.execution.split;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.query.execution.split.SplitDrainer;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.ConnectorSplitSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests verifying SplitDrainer correctly drains all batches
 * from a paged ConnectorSplitSource.
 */
class SplitDrainerPropertyTest {

    @Property(tries = 10)
    void drainsAllSplitsAcrossMultipleBatches(
            @ForAll("pagedSplitCounts") List<Integer> batchSizes) {
        int totalExpected = batchSizes.stream().mapToInt(Integer::intValue).sum();
        PagedSplitSource source = new PagedSplitSource(batchSizes);

        List<ConnectorSplit> result = SplitDrainer.drainAll(source, 1024);

        assertThat(result).hasSize(totalExpected);
        assertThat(source.isFinished()).isTrue();
    }

    @Property(tries = 10)
    void emptySourceReturnsEmptyList(@ForAll("emptySourceFlag") boolean flag) {
        PagedSplitSource source = new PagedSplitSource(List.of());

        List<ConnectorSplit> result = SplitDrainer.drainAll(source);

        assertThat(result).isEmpty();
        assertThat(source.isFinished()).isTrue();
    }

    @Property(tries = 10)
    void singleBatchSourceReturnsSameSplits(
            @ForAll("singleBatchSize") int size) {
        PagedSplitSource source = new PagedSplitSource(List.of(size));

        List<ConnectorSplit> result = SplitDrainer.drainAll(source);

        assertThat(result).hasSize(size);
    }

    @Provide
    Arbitrary<List<Integer>> pagedSplitCounts() {
        return Arbitraries.integers().between(0, 50)
                .list().ofMinSize(1).ofMaxSize(10);
    }

    @Provide
    Arbitrary<Boolean> emptySourceFlag() {
        return Arbitraries.of(true, false);
    }

    @Provide
    Arbitrary<Integer> singleBatchSize() {
        return Arbitraries.integers().between(1, 200);
    }

    /**
     * A ConnectorSplitSource that returns pre-configured batches of stub splits,
     * simulating paged split enumeration.
     */
    private static final class PagedSplitSource implements ConnectorSplitSource {
        private final List<List<ConnectorSplit>> batches;
        private int batchIndex;

        PagedSplitSource(List<Integer> batchSizes) {
            this.batches = new ArrayList<>();
            for (int size : batchSizes) {
                List<ConnectorSplit> batch = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    batch.add(new StubSplit());
                }
                batches.add(batch);
            }
        }

        @Override
        public List<ConnectorSplit> getNextBatch(int maxSize) {
            if (batchIndex >= batches.size()) {
                return List.of();
            }
            return batches.get(batchIndex++);
        }

        @Override
        public boolean isFinished() {
            return batchIndex >= batches.size();
        }

        @Override
        public void close() {}
    }

    private static final class StubSplit implements ConnectorSplit {
        // All methods use default implementations from ConnectorSplit
    }
}
