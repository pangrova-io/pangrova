package org.pangrova.parser.api.ast;

import java.util.Objects;

/**
 * Represents a system table reference parsed from the {@code table$systemTable} syntax.
 *
 * <p>When a qualified name in a FROM clause contains a {@code DOLLAR_IDENTIFIER}
 * (e.g., {@code orders$snapshots} or {@code catalog.schema.orders$files}), the parser
 * splits it into a base table name and a system table name. Planning preserves the
 * suffix in the relation identity; connector metadata binds an ordinary Native
 * scan source, with authorization checked against the base table.</p>
 *
 * @param baseTable      the qualified base table name (e.g., {@code catalog.schema.orders})
 * @param systemTableName the system table suffix after {@code $} (e.g., {@code snapshots}, {@code files})
 */
public record SystemTableReference(
        Identifier baseTable,
        String systemTableName) {

    public SystemTableReference {
        Objects.requireNonNull(baseTable, "baseTable must not be null");
        Objects.requireNonNull(systemTableName, "systemTableName must not be null");
        if (systemTableName.isEmpty()) {
            throw new IllegalArgumentException("systemTableName must not be empty");
        }
    }
}
