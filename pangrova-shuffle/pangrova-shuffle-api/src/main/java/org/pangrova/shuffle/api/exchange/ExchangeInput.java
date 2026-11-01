package org.pangrova.shuffle.api.exchange;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Complete, task-scoped binding for one typed ExchangeInput plan node. */
public record ExchangeInput(
        String planNodeId,
        String exchangeId,
        int expectedSourcePartitions,
        ExchangeInputTransport transport) {

    public ExchangeInput {
        requireNonBlank(planNodeId, "planNodeId");
        requireNonBlank(exchangeId, "exchangeId");
        if (transport == null) {
            throw new IllegalArgumentException("transport must not be null");
        }
        if (expectedSourcePartitions <= 0) {
            throw new IllegalArgumentException(
                    "expectedSourcePartitions must be positive");
        }
        if (transport instanceof DirectExchangeTransport direct) {
            validateDirectSources(direct.sources(), expectedSourcePartitions);
        } else if (expectedSourcePartitions != 1) {
            throw new IllegalArgumentException(
                    "external shuffle exposes exactly one partition stream");
        }
    }

    private static void validateDirectSources(
            List<ExchangeEndpoint> sources, int expectedSourcePartitions) {
        if (sources.size() != expectedSourcePartitions) {
            throw new IllegalArgumentException(
                    "source count does not match expectedSourcePartitions");
        }
        Set<LogicalProducer> producers = new HashSet<>();
        for (ExchangeEndpoint source : sources) {
            LogicalProducer producer = new LogicalProducer(
                    source.sourceStageId(), source.sourcePartitionId());
            if (!producers.add(producer)) {
                throw new IllegalArgumentException(
                        "duplicate logical producer: stage="
                                + source.sourceStageId() + " partition="
                                + source.sourcePartitionId());
            }
        }
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private record LogicalProducer(int stageId, int partitionId) {}
}
