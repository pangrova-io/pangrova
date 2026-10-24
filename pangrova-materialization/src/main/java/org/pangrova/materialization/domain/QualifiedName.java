package org.pangrova.materialization.domain;

import java.util.List;

/**
 * Dot-separated SQL object name.
 *
 * <p>Parts are kept separate so that identifiers containing dots (quoted SQL
 * identifiers) cannot be confused with qualified names during resolution.
 *
 * @param parts ordered name parts, from catalog to table
 */
public record QualifiedName(List<String> parts) {

    public QualifiedName {
        if (parts == null || parts.isEmpty()) {
            throw new IllegalArgumentException("qualified name must have at least one part");
        }
        if (parts.size() > 4) {
            throw new IllegalArgumentException("qualified name must have at most four parts");
        }
        parts = List.copyOf(parts.stream().map(part -> Require.text(part, "name part")).toList());
    }

    /**
     * Creates a qualified name from its dot-separated representation.
     *
     * @param name dot-separated name
     * @return parsed qualified name
     */
    public static QualifiedName parse(String name) {
        Require.text(name, "name");
        return new QualifiedName(List.of(name.split("\\.", -1)));
    }

    /**
     * Returns the last (table) name part.
     *
     * @return table name
     */
    public String tableName() {
        return parts.get(parts.size() - 1);
    }

    /**
     * Returns the parent name without the last part.
     *
     * @return parent qualified name, or empty for a single-part name
     */
    public java.util.Optional<QualifiedName> parent() {
        if (parts.size() == 1) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(new QualifiedName(parts.subList(0, parts.size() - 1)));
    }

    @Override
    public String toString() {
        return String.join(".", parts);
    }
}
