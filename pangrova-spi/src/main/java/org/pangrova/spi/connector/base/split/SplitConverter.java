package org.pangrova.spi.connector.base.split;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;

import java.util.List;

/**
 * Converts a table handle into connector-specific splits for parallel reading.
 */
@Experimental
public interface SplitConverter {

    /**
     * Returns the source type this converter handles (e.g., "iceberg", "paimon").
     */
    String getSourceType();

    /**
     * Converts a table handle into a list of splits for parallel execution.
     */
    List<ConnectorSplit> convert(ConnectorSession session, ConnectorTableHandle table);
}
