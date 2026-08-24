package org.pangrova.worker.transport;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.transport.TransportStatus;

import java.util.Map;

/**
 * Maps PangrovaErrorCode to TransportStatus using error code prefix classification.
 * Self-contained — does not depend on pangrova-resilience ErrorClassifier.
 */
public final class TransportStatusMapper {

    private static final Map<String, TransportStatus> PREFIX_MAP = Map.ofEntries(
            Map.entry("VALIDATION", TransportStatus.INVALID_ARGUMENT),
            Map.entry("PARSER", TransportStatus.INVALID_ARGUMENT),
            Map.entry("PLANNER", TransportStatus.INVALID_ARGUMENT),
            Map.entry("RESOURCE", TransportStatus.RESOURCE_EXHAUSTED),
            Map.entry("THROTTLE", TransportStatus.RESOURCE_EXHAUSTED),
            Map.entry("RUNTIME", TransportStatus.INTERNAL),
            Map.entry("CONFIG", TransportStatus.INTERNAL),
            Map.entry("NATIVE", TransportStatus.INTERNAL),
            Map.entry("SUBSTRAIT", TransportStatus.INTERNAL),
            Map.entry("METADATA", TransportStatus.UNAVAILABLE),
            Map.entry("EXCHANGE", TransportStatus.UNAVAILABLE),
            Map.entry("FLIGHT", TransportStatus.UNAVAILABLE),
            Map.entry("CACHE", TransportStatus.UNAVAILABLE)
    );

    private TransportStatusMapper() {
        // utility class
    }

    /**
     * Maps an PangrovaErrorCode to a TransportStatus based on its code prefix.
     * Falls back to {@link TransportStatus#INTERNAL} for unknown prefixes.
     */
    public static TransportStatus map(PangrovaErrorCode errorCode) {
        String prefix = errorCode.getCode().split("-")[0];
        return PREFIX_MAP.getOrDefault(prefix, TransportStatus.INTERNAL);
    }
}
