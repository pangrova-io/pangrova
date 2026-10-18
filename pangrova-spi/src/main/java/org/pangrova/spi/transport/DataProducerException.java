package org.pangrova.spi.transport;

import java.util.Objects;

/**
 * Thrown by {@link DataProducer#openStream(DataTicket)} or {@link DataStream#next()}
 * to signal a transport-level
 * error that is distinct from normal EOF (empty bytes) or not-ready (null).
 *
 * <p>Typical use: queryId not found in result buffer → {@link TransportStatus#NOT_FOUND}.
 * The transport layer translates this into the appropriate wire-level error
 * (e.g., gRPC NOT_FOUND status) so clients receive an explicit error
 * instead of a false-success empty result.</p>
 */
public final class DataProducerException extends RuntimeException {

    private final TransportStatus status;

    public DataProducerException(TransportStatus status, String message) {
        super(message);
        this.status = Objects.requireNonNull(status, "status");
    }

    public TransportStatus getStatus() {
        return status;
    }
}
