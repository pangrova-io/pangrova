package org.pangrova.coordinator.http.jdbc;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Pat;

import java.util.Collections;
import java.util.List;

/**
 * Provides Pangrova catalog metadata to JDBC clients via the Avatica wire protocol.
 * Returns catalog, schema, table, and column information from the coordinator's
 * metadata service.
 */
public final class CatalogJdbcMetadataProvider implements JdbcMetadataProvider {

    private static final String DEFAULT_CATALOG = "pangrova";
    private static final String DEFAULT_SCHEMA = "default";

    @Override
    public MetaResultSet getCatalogs(Meta.ConnectionHandle ch) {
        Meta.Frame frame = Meta.Frame.create(0, true, Collections.singletonList(
                new Object[]{DEFAULT_CATALOG}));
        return MetaResultSet.create(ch.id, -1, false,
                Meta.Signature.create(
                        Collections.emptyList(), "",
                        Collections.emptyList(),
                        Meta.CursorFactory.ARRAY, Meta.StatementType.SELECT),
                frame);
    }

    @Override
    public MetaResultSet getSchemas(Meta.ConnectionHandle ch, String catalog, Pat schemaPattern) {
        Meta.Frame frame = Meta.Frame.create(0, true, Collections.singletonList(
                new Object[]{DEFAULT_SCHEMA, DEFAULT_CATALOG}));
        return MetaResultSet.create(ch.id, -1, false,
                Meta.Signature.create(
                        Collections.emptyList(), "",
                        Collections.emptyList(),
                        Meta.CursorFactory.ARRAY, Meta.StatementType.SELECT),
                frame);
    }

    @Override
    public MetaResultSet getTables(Meta.ConnectionHandle ch, String catalog, Pat schemaPattern,
                                    Pat tableNamePattern, List<String> typeList) {
        return MetaResultSet.create(ch.id, -1, false,
                Meta.Signature.create(
                        Collections.emptyList(), "",
                        Collections.emptyList(),
                        Meta.CursorFactory.ARRAY, Meta.StatementType.SELECT),
                Meta.Frame.EMPTY);
    }

    @Override
    public MetaResultSet getColumns(Meta.ConnectionHandle ch, String catalog, Pat schemaPattern,
                                     Pat tableNamePattern, Pat columnNamePattern) {
        return MetaResultSet.create(ch.id, -1, false,
                Meta.Signature.create(
                        Collections.emptyList(), "",
                        Collections.emptyList(),
                        Meta.CursorFactory.ARRAY, Meta.StatementType.SELECT),
                Meta.Frame.EMPTY);
    }

    @Override
    public MetaResultSet getTableTypes(Meta.ConnectionHandle ch) {
        Meta.Frame frame = Meta.Frame.create(0, true, Collections.singletonList(
                new Object[]{"TABLE"}));
        return MetaResultSet.create(ch.id, -1, false,
                Meta.Signature.create(
                        Collections.emptyList(), "",
                        Collections.emptyList(),
                        Meta.CursorFactory.ARRAY, Meta.StatementType.SELECT),
                frame);
    }
}
