package org.pangrova.metadata.adapter;

import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;

/**
 * Lightweight table handle for federation catalogs.
 */
record FederationTableHandle(String schema, String table) implements ConnectorTableHandle {
}
