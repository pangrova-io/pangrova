package org.pangrova.spi.connector.metadata.table;

import org.pangrova.spi.annotation.Experimental;

/**
 * Immutable connector-owned relation expanded before query authorization and
 * optimization. Its bound declaration is the exact public output schema.
 * Expansion yields ordinary Native input scans; this handle is never a split.
 */
@Experimental
public interface ConnectorDerivedTableHandle extends ConnectorTableHandle {
    ConnectorDerivedQuery derivedQuery();
}
