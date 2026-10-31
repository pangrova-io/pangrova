package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Connector-owned, versioned codec for Coordinator-allocated output handles.
 * Implementations must be deterministic and reject unknown versions, trailing
 * bytes, malformed lengths, and handles belonging to another connector.
 */
@Experimental
public interface ConnectorOutputHandleCodec {

    String typeUrl();

    int codecVersion();

    byte[] serialize(ConnectorOutputTableHandle handle);

    ConnectorOutputTableHandle deserialize(int version, byte[] payload);

    /** Exact ordered write columns pinned by this output handle. */
    java.util.List<String> writeColumnNames(ConnectorOutputTableHandle handle);
}
