package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.util.Optional;

/**
 * Marker interface for connector-specific output table handle.
 *
 * <p>Each connector implementation provides its own class implementing this interface
 * to represent a reference to a table being written to in the underlying data source.</p>
 */
@Experimental
public interface ConnectorOutputTableHandle {

    /**
     * A-COMMIT-01: the target table this handle writes to, if the handle carries it. Lets a
     * recovering coordinator record a structured, rehydratable identity in the write-commit
     * journal (instead of a lossy {@code toString()}), so the write can be re-driven after a crash.
     *
     * <p>Default empty; connector handles that know their table override this.
     */
    default Optional<SchemaTableName> targetTable() {
        return Optional.empty();
    }

    /**
     * A-COMMIT-01: the distributed write mode this handle represents (connector-specific token,
     * e.g. INSERT/OVERWRITE/DELETE), or {@code "UNKNOWN"} if not carried. Used to reconstruct an
     * equivalent handle during crash recovery.
     */
    default String writeMode() {
        return "UNKNOWN";
    }
}
