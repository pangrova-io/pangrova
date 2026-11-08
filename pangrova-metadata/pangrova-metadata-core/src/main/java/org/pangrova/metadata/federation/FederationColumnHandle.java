package org.pangrova.metadata.federation;

import org.pangrova.connector.api.metadata.table.ColumnHandle;

/**
 * Lightweight column handle for federation catalogs, carrying the column name and type.
 */
record FederationColumnHandle(String columnName, String columnType) implements ColumnHandle {
}
