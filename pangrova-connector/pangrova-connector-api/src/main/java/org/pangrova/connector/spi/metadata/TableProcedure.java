package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Map;

/**
 * Describes a connector-specific table maintenance procedure.
 *
 * <p>A {@code TableProcedure} is invoked via {@code CALL catalog.procedure_name(args...)}
 * syntax. The coordinator resolves the catalog prefix, maps the procedure name through
 * {@link ConnectorMetadata#listTableProcedures()}, and dispatches to
 * {@link ConnectorMetadata#executeProcedure(ConnectorSession, String, Map)}.
 *
 * <p>This is the Pangrova equivalent of Trino's {@code ALTER TABLE ... EXECUTE procedure}
 * and Spark's {@code CALL catalog.system.procedure_name(args...)} convention.
 *
 * <p>Implementations are typically returned by
 * {@link ConnectorMetadata#listTableProcedures()} so the engine can expose them
 * in information-schema style queries and SQL autocompletion.</p>
 *
 * @see ConnectorMetadata#executeProcedure
 * @see ConnectorMetadata#listTableProcedures
 */
@Experimental
public interface TableProcedure {

    /**
     * Returns the procedure name (case-insensitive, snake_case by convention).
     *
     * <p>Examples: {@code "optimize"}, {@code "expire_snapshots"},
     * {@code "remove_orphan_files"}, {@code "rewrite_manifests"},
     * {@code "rollback_to_snapshot"}, {@code "compact"}.
     */
    String name();

    /**
     * Returns a human-readable description of this procedure.
     */
    String description();

    /**
     * Returns the property definitions for this procedure.
     *
     * <p>Each property has a name, a description, whether it is required,
     * and an optional default value. The engine uses these to validate
     * user-supplied arguments before dispatching to
     * {@link ConnectorMetadata#executeProcedure}.</p>
     */
    List<PropertyDefinition> properties();

    /**
     * Describes a single procedure argument/property.
     *
     * @param name         the argument name (case-insensitive)
     * @param description  human-readable description
     * @param required     whether this argument is required
     * @param defaultValue optional default value (null if none)
     */
    record PropertyDefinition(
            String name,
            String description,
            boolean required,
            String defaultValue) {

        /**
         * Creates a required property with no default.
         */
        public static PropertyDefinition required(String name, String description) {
            return new PropertyDefinition(name, description, true, null);
        }

        /**
         * Creates an optional property with a default value.
         */
        public static PropertyDefinition optional(String name, String description, String defaultValue) {
            return new PropertyDefinition(name, description, false, defaultValue);
        }
    }
}
