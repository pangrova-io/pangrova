package org.pangrova.table.api.ownership;

import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.table.api.enums.TableControlState;

import java.util.Optional;
import java.util.UUID;

/**
 * Read model for table resolution: ownership plus the identity/version evidence
 * available for controlled tables.
 */
public record TableControlView(
        OwnershipMode ownershipMode,
        Optional<UUID> tableId,
        Optional<String> versionToken,
        Optional<TableControlState> state,
        long metadataRevision) {

    public TableControlView {
        tableId = tableId == null ? Optional.empty() : tableId;
        versionToken = versionToken == null ? Optional.empty() : versionToken;
        state = state == null ? Optional.empty() : state;
    }

    /** A table that is visible in the catalog but has no control record. */
    public static TableControlView observed() {
        return new TableControlView(
                OwnershipMode.OBSERVED, Optional.empty(), Optional.empty(), Optional.empty(), 0L);
    }

    /** Projects a persisted control record into the resolution read model. */
    public static TableControlView of(TableControlRecord record) {
        return new TableControlView(
                record.ownershipMode(),
                Optional.of(record.tableId()),
                Optional.ofNullable(record.versionToken()),
                Optional.of(record.state()),
                record.metadataRevision());
    }

    /** True when the control state permits writes and destructive DDL. */
    public boolean isManagedActive() {
        return ownershipMode == OwnershipMode.MANAGED
                && state.orElse(TableControlState.ACTIVE) == TableControlState.ACTIVE;
    }
}
