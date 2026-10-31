package org.pangrova.metadata.adapter;

import org.pangrova.connector.spi.metadata.table.ColumnHandle;

/**
 * Lightweight column handle for federation catalogs, carrying the column name and type.
 */
record FederationColumnHandle(String columnName, String columnType) implements ColumnHandle {
}
