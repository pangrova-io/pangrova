package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.util.Optional;
import java.util.OptionalLong;

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

    /**
     * Snapshot captured when this write began, if the connector uses snapshot isolation.
     * Recovery must use this value rather than reconstructing from the live table snapshot.
     */
    default OptionalLong writeBaseSnapshotId() {
        return OptionalLong.empty();
    }

    /**
     * Connector-owned Native dataset allocation, if this handle uses a Native sink.
     * The exact same allocation must be reconstructed by the output handle codec.
     *
     * @return immutable dataset allocation, or empty for connector PageSink writes
     */
    default Optional<NativeWriteAllocation> nativeWriteAllocation() {
        return Optional.empty();
    }

    /** Exact catalog-accepted base required by a positive Native Dataset write allocation. */
    default Optional<AcceptedSourceSnapshot> acceptedWriteBaseSnapshot() {
        return Optional.empty();
    }

    /** Exact physical-file authority; mutually exclusive with a Native Dataset allocation. */
    default Optional<NativePhysicalWriteAllocation> nativePhysicalWriteAllocation() {
        return Optional.empty();
    }

    /**
     * Format identity captured when this write began.
     *
     * <p>Commit and recovery must compare this captured evidence with the live
     * table; an empty value means the write cannot be fenced against a
     * drop/recreate and must not be treated as matching.</p>
     */
    default Optional<org.pangrova.connector.api.metadata.table.TableIdentity> tableIdentity() {
        return Optional.empty();
    }
}
