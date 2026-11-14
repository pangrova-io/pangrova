package org.pangrova.metadata.api.relation;

import org.pangrova.metadata.api.enums.ObjectKind;

import org.pangrova.metadata.api.object.QualifiedTableRef;

import java.util.Objects;

/**
 * Four-part identifier for a catalog object that may participate in a
 * dependency edge: {@code (catalog, schema, name, kind)}.
 *
 * <p>Distinct from {@link QualifiedTableRef} which is table-only and unkinded;
 * dependency tracking spans non-table kinds (views, MVs, streams, functions),
 * so it carries an explicit {@link ObjectKind}.
 * Record value equality includes kind; the dependency store separately
 * keys graph objects by their three-part natural name.
 */
public record ObjectRef(String catalog, String schema, String name, ObjectKind kind) {

    public ObjectRef {
        Objects.requireNonNull(catalog, "catalog must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(kind, "kind must not be null");
        if (catalog.isBlank() || schema.isBlank() || name.isBlank()) {
            throw new IllegalArgumentException(
                    "catalog/schema/name must be non-blank: catalog='%s' schema='%s' name='%s'"
                            .formatted(catalog, schema, name));
        }
    }

    /** Convenience: materialize a {@code TABLE}-kinded ref from a {@link QualifiedTableRef}. */
    public static ObjectRef ofTable(QualifiedTableRef ref) {
        Objects.requireNonNull(ref, "ref must not be null");
        return new ObjectRef(ref.catalog(), ref.schema(), ref.table(), ObjectKind.TABLE);
    }

    /** Convenience: materialize a ref of an arbitrary kind from a {@link QualifiedTableRef}. */
    public static ObjectRef of(QualifiedTableRef ref, ObjectKind kind) {
        Objects.requireNonNull(ref, "ref must not be null");
        return new ObjectRef(ref.catalog(), ref.schema(), ref.table(), kind);
    }

    /** Adapt back to a {@link QualifiedTableRef} (loses {@link #kind}). */
    public QualifiedTableRef toTableRef() {
        return new QualifiedTableRef(catalog, schema, name);
    }

    @Override
    public String toString() {
        return kind + ":" + catalog + "." + schema + "." + name;
    }
}
