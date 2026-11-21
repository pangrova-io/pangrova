package org.pangrova.query.session;

import org.pangrova.query.api.session.QuerySession;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.util.List;

/** Binds table names to query defaults; authorization remains the caller's responsibility. */
public final class SessionTableNames {
    private SessionTableNames() {}

    public static QualifiedTableName resolveTableRef(
            List<String> parts, QuerySession context) {
        return QualifiedTableName.resolve(parts, defaultCatalog(context), defaultSchema(context));
    }

    public static String defaultCatalog(QuerySession context) {
        return context.sessionProperties().getOrDefault("catalog", "default");
    }

    public static String defaultSchema(QuerySession context) {
        return context.sessionProperties().getOrDefault("schema", "default");
    }
}
