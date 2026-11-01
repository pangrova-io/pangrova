package org.pangrova.spi.connector.metadata.table;

import org.pangrova.common.api.annotation.Experimental;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * Positive format evidence for the exact schema selected by a read binding.
 *
 * <p>The stability declaration distinguishes IDs meaningful only within this exact
 * selected source from IDs the format will not reassign within the physical table
 * and field namespace. A namespace includes any relevant branch boundary. Missing
 * evidence remains an empty optional; ordinals are never format identity evidence.
 * A SELECTED_SOURCE value must stay associated with its binding's source identity;
 * it cannot authorize reuse across different reads or versions by itself.
 * Nested field identity is not certified here.</p>
 *
 * @param tableIdentity physical owner and selected schema fingerprint, captured with the declaration
 * @param fieldNamespace connector-qualified allocator namespace, independent of diagnostic table names
 * @param fields exact immutable top-level fields from the selected declaration
 * @param stability scope of the format's demonstrated field-ID allocation guarantee
 */
@Experimental
public record ConnectorReadSchemaIdentity(TableIdentity tableIdentity, String fieldNamespace,
        List<ConnectorTableMetadata.Column> fields, FieldIdStability stability) {
    public enum FieldIdStability { SELECTED_SOURCE, NON_REUSABLE_NAMESPACE }

    public ConnectorReadSchemaIdentity {
        Objects.requireNonNull(tableIdentity, "tableIdentity");
        Objects.requireNonNull(stability, "stability");
        if (!tableIdentity.hasFormatUuid() || tableIdentity.schemaFingerprint().isBlank()) {
            throw new IllegalArgumentException("Read schema identity requires physical and schema evidence");
        }
        if (fieldNamespace == null || fieldNamespace.isBlank()) {
            throw new IllegalArgumentException("Read schema identity requires a field allocator namespace");
        }
        fields = List.copyOf(fields);
        var ids = new HashSet<Integer>();
        for (var field : fields) {
            if (field.fieldId().isEmpty() || field.fieldId().getAsInt() < 0
                    || !ids.add(field.fieldId().getAsInt())) {
                throw new IllegalArgumentException("Read schema identity requires unique nonnegative format field IDs");
            }
        }
    }

    /** Omits locations, schema contents and provider credentials. */
    @Override
    public String toString() {
        return "ConnectorReadSchemaIdentity[format=" + tableIdentity.format() + ", fields=" + fields.size() + "]";
    }
}
