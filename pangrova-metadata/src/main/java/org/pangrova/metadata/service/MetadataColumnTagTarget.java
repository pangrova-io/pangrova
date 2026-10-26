package org.pangrova.metadata.service;

import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.control.TableControlRecord;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.security.AccessDeniedException;

import java.util.Objects;
import java.util.Optional;

/** Resolves a tag's canonical column under the caller's connector session and captured control owner. */
record MetadataColumnTagTarget(TableControlRecord owner, String column, Optional<PangrovaType> type) {

    static MetadataColumnTagTarget resolve(CatalogRegistry catalogs, MetadataSessionContext context,
            QualifiedTableRef table, String requestedColumn) {
        Objects.requireNonNull(requestedColumn, "column");
        var owner = catalogs.tableGovernance().requireBindable(table.catalog(), table.schema(), table.table());
        var session = DefaultConnectorSession.fromSessionContext(context, table.catalog(), table.schema());
        var metadata = catalogs.getCatalog(table.catalog()).getMetadata(session);
        var name = new SchemaTableName(table.schema(), table.table());
        var handle = metadata.getTableHandle(session, name);
        if (handle == null) throw new AccessDeniedException("Column tag target table is unavailable");
        var control = catalogs.tableControl();
        var identity = metadata.getTableIdentity(session, handle);
        control.requireReadIdentity(owner, identity, "Column tags");
        var declaration = metadata.getTableMetadata(session, handle);
        // A second live lookup rejects replacement during declaration resolution.
        var current = metadata.getTableHandle(session, name);
        if (current == null) throw new AccessDeniedException("Column tag target table is unavailable");
        var after = metadata.getTableIdentity(session, current);
        control.requireReadIdentity(owner, after, "Column tags");
        if (identity.orElseThrow().schemaFingerprint().isBlank()
                || !identity.orElseThrow().schemaFingerprint().equals(after.orElseThrow().schemaFingerprint())) {
            throw new AccessDeniedException("Column tag schema evidence is unavailable or changed during resolution");
        }
        String column = canonicalColumn(declaration, requestedColumn);
        var type = declaration.typedColumns().stream().filter(field -> field.name().equals(column))
                .map(ConnectorTableMetadata.Column::type).findFirst();
        return new MetadataColumnTagTarget(owner, column, type);
    }

    private static String canonicalColumn(ConnectorTableMetadata declaration, String requested) {
        if (declaration.columns().contains(requested)) return requested;
        var matches = declaration.columns().stream().filter(name -> name.equalsIgnoreCase(requested)).toList();
        if (matches.isEmpty()) throw new AccessDeniedException("Column not found: " + requested);
        if (matches.size() != 1) throw new AccessDeniedException("Column tag target is ambiguous: " + requested);
        return matches.get(0);
    }

    void requireTagType(String key) {
        if ("pii_type".equals(key) && type.orElse(null) != PangrovaType.VARCHAR) {
            throw new AccessDeniedException("PII classification masks require a VARCHAR column with a declared type");
        }
    }
}
