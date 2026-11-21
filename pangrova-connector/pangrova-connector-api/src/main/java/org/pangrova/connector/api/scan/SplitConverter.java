package org.pangrova.connector.api.scan;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;

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
