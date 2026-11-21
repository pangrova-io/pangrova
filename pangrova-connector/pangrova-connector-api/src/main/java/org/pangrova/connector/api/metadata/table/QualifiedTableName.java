package org.pangrova.connector.api.metadata.table;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Immutable representation of a fully-qualified table name (catalog.schema.table).
 * Replaces duplicate resolution logic previously scattered across DmlWriteHelper
 * and SqlCompiler.
 */
@Experimental
public record QualifiedTableName(String catalog, String schema, String table) {

    /**
     * Resolves a qualified table name from a list of dot-separated parts.
     *
     * @param parts 1 to 3 name parts: [table], [schema, table], or [catalog, schema, table]
     * @return resolved name with "default" filling missing catalog/schema
     * @throws IllegalArgumentException if parts is empty or has more than 3 elements
     */
    public static QualifiedTableName resolve(List<String> parts) {
        return resolve(parts, "default", "default");
    }

    /**
     * Constructs a name from one to three exact components and explicit defaults.
     * Preserves case, dots and component boundaries; performs no lookup or authorization.
     * Defaults are used only for omitted components; this is not a SQL string parser.
     */
    public static QualifiedTableName resolve(List<String> parts, String defaultCatalog, String defaultSchema) {
        return switch (parts.size()) {
            case 3 -> new QualifiedTableName(parts.get(0), parts.get(1), parts.get(2));
            case 2 -> new QualifiedTableName(defaultCatalog, parts.get(0), parts.get(1));
            case 1 -> new QualifiedTableName(defaultCatalog, defaultSchema, parts.get(0));
            default -> throw new IllegalArgumentException("Invalid table name: " + parts);
        };
    }

    /**
     * Resolves a qualified table name, returning null for invalid input
     * instead of throwing. Used by access-control paths where a missing
     * table name is non-fatal.
     */
    public static QualifiedTableName resolveOrNull(List<String> parts) {
        return switch (parts.size()) {
            case 3 -> new QualifiedTableName(parts.get(0), parts.get(1), parts.get(2));
            case 2 -> new QualifiedTableName("default", parts.get(0), parts.get(1));
            case 1 -> new QualifiedTableName("default", "default", parts.get(0));
            default -> null;
        };
    }

    @Override
    public String toString() {
        return catalog + "." + schema + "." + table;
    }

    /**
     * A component-preserving key for table-specific feedback, not SQL or source authority.
     * Each component is length-prefixed in Java UTF-16 units; no splitting or case folding occurs.
     */
    public String canonicalKey() {
        var key = new StringBuilder("TABLE/");
        for (String component : java.util.Arrays.asList(catalog, schema, table)) {
            java.util.Objects.requireNonNull(component, "table identifier component");
            key.append(component.length()).append(':').append(component);
        }
        return key.toString();
    }
}
