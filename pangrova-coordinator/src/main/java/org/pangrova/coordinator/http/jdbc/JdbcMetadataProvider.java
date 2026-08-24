package org.pangrova.coordinator.http.jdbc;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Pat;

import java.util.List;

/**
 * Provides database metadata (catalogs, schemas, tables, columns) for JDBC clients.
 * Maps Pangrova's catalog structure to the Avatica metadata result format.
 */
public interface JdbcMetadataProvider {

    MetaResultSet getCatalogs(Meta.ConnectionHandle ch);

    MetaResultSet getSchemas(Meta.ConnectionHandle ch, String catalog, Pat schemaPattern);

    MetaResultSet getTables(Meta.ConnectionHandle ch, String catalog, Pat schemaPattern,
                            Pat tableNamePattern, List<String> typeList);

    MetaResultSet getColumns(Meta.ConnectionHandle ch, String catalog, Pat schemaPattern,
                             Pat tableNamePattern, Pat columnNamePattern);

    MetaResultSet getTableTypes(Meta.ConnectionHandle ch);
}
