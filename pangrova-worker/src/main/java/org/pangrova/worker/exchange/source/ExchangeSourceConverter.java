package org.pangrova.worker.exchange.source;

import java.util.List;

import org.pangrova.protocol.rest.task.ExchangeSourceDescriptor;
import org.pangrova.worker.exchange.ExchangeEndpoint;
import org.pangrova.worker.exchange.ExchangeType;

/**
 * Converts protocol-layer ExchangeSourceDescriptor to internal worker ExchangeEndpoint
 * and SPI ExchangeEndpoint types.
 * Anti-corruption layer: protocol types do not leak into exchange internals.
 */
public final class ExchangeSourceConverter {

    private ExchangeSourceConverter() {
    }

    /**
     * Converts protocol descriptors to SPI exchange endpoints.
     */
    public static List<org.pangrova.shuffle.exchange.ExchangeEndpoint> toSpiEndpoints(
            List<ExchangeSourceDescriptor> protocolSources) {
        if (protocolSources == null || protocolSources.isEmpty()) {
            return List.of();
        }
        return protocolSources.stream()
                .map(src -> new org.pangrova.shuffle.exchange.ExchangeEndpoint(
                        src.sourceTaskId(), src.host(), src.transportPort(), src.partitionId()))
                .toList();
    }

    /**
     * Converts a list of protocol exchange source descriptors to internal exchange endpoints.
     */
    public static List<ExchangeEndpoint> convert(
            List<ExchangeSourceDescriptor> protocolSources) {
        if (protocolSources == null || protocolSources.isEmpty()) {
            return List.of();
        }
        return protocolSources.stream()
                .map(ExchangeSourceConverter::convertOne)
                .toList();
    }

    private static ExchangeEndpoint convertOne(ExchangeSourceDescriptor src) {
        return new ExchangeEndpoint(
                src.sourceTaskId(),
                src.host(),
                src.transportPort(),
                src.partitionId(),
                mapExchangeType(src.exchangeType()),
                src.sequenceId());
    }

    private static ExchangeType mapExchangeType(String wireType) {
        if (wireType == null || wireType.isBlank()) {
            return ExchangeType.HASH_PARTITION;
        }
        try {
            return ExchangeType.valueOf(wireType.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ExchangeType.HASH_PARTITION;
        }
    }
}
