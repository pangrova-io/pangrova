package org.pangrova.metadata.federation;

import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;

/**
 * Lightweight table handle for federation catalogs.
 */
record FederationTableHandle(String schema, String table) implements ConnectorTableHandle {
}
