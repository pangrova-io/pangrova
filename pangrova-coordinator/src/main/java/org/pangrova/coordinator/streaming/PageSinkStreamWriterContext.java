package org.pangrova.coordinator.streaming;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;

import java.util.Objects;

/**
 * Aggregated dependencies for {@link ConnectorPageSinkCdcWriter}.
 *
 * <p>Keeps the writer constructor at 1 param (this record).</p>
 *
 * @param metadata           connector metadata for transaction management
 * @param pageSinkProvider   creates page sinks for the standard write path
 * @param session            connector session context
 * @param outputTableHandle  handle to the target output table
 * @param targetTable        fully qualified target table name
 */
public record PageSinkStreamWriterContext(
        ConnectorMetadata metadata,
        ConnectorPageSinkProvider pageSinkProvider,
        ConnectorSession session,
        ConnectorOutputTableHandle outputTableHandle,
        String targetTable
) {

    public PageSinkStreamWriterContext {
        Objects.requireNonNull(metadata, "metadata must not be null");
        Objects.requireNonNull(pageSinkProvider, "pageSinkProvider must not be null");
        Objects.requireNonNull(session, "session must not be null");
        Objects.requireNonNull(outputTableHandle, "outputTableHandle must not be null");
        Objects.requireNonNull(targetTable, "targetTable must not be null");
    }
}
