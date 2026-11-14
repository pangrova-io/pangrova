package org.pangrova.table.rename;

import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableVersionToken;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.table.api.observation.TableVersionObservation;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.ownership.TableControlStore;
import org.pangrova.table.api.rename.TableControlRenameIntent;
import org.pangrova.table.config.TableControlSettings;
import org.pangrova.table.identity.TableCatalogAuthority;
import org.pangrova.table.identity.TablePhysicalIdentity;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

/** Owns the durable rename intent and its physically verified completion. */
public final class TableRenames {
    private final TableControlStore store;
    private final TableCatalogAuthority catalogs;
    private final TableControlSettings settings;
    private final Clock clock;

    public TableRenames(TableControlStore store, TableCatalogAuthority catalogs, TableControlSettings settings, Clock clock) {
        this.store = store;
        this.catalogs = catalogs;
        this.settings = settings;
        this.clock = clock;
    }

    /**
     * Moves the control record with an in-place rename so ownership survives the
     * move. A live record already registered under the target name blocks the
     * move instead of being overwritten: two tables cannot share a name, and
     * silently replacing the target's ownership would strand its governance.
     */
    public void begin(String catalog, String schema, String table,
            String newSchema, String newTable) {
        if (!settings.enabled()) {
            return;
        }
        Optional<TableControlRecord> source = store.find(catalog, schema, table);
        if (source.isEmpty()) {
            return;
        }
        begin(source.get(), newSchema, newTable);
    }

    /** Moves only the exact captured registration, including when rolling back a failed rename. */
    public TableControlRecord begin(TableControlRecord expected, String newSchema, String newTable) {
        if (!settings.enabled()) {
            return expected;
        }
        TableControlRecord source = store.findById(expected.tableId()).orElse(null);
        if (source == null || source.metadataRevision() != expected.metadataRevision()
                || source.state() == TableControlState.DROPPED) {
            throw new AccessDeniedException("RENAME rejected: control record changed for "
                    + expected.qualifiedName() + "; refusing to move a newer table registration");
        }
        catalogs.requireCurrent(source, "RENAME");
        Optional<TableControlRecord> target = store.find(source.catalog(), newSchema, newTable);
        if (target.isPresent() && !target.get().tableId().equals(source.tableId())) {
            throw new AccessDeniedException("RENAME rejected: " + source.catalog() + "." + newSchema
                    + "." + newTable + " already has a live control record (tableId="
                    + target.get().tableId() + ")");
        }
        TableControlRecord moved = source.toBuilder()
                .state(TableControlState.RENAMING)
                .properties(TableControlRenameIntent.begin(source))
                .name(source.catalog(), newSchema, newTable)
                .metadataRevision(source.metadataRevision() + 1)
                .updatedAt(clock.instant())
                .build();
        store.save(moved);
        return moved;
    }

    /** Records physical identity changes caused by a successfully completed, admitted rename. */
    public void complete(TableControlRecord expected, Optional<TableIdentity> identity,
            Optional<TableVersionToken> version) {
        if (!settings.enabled()) {
            return;
        }
        TableControlRecord current = store.findById(expected.tableId()).orElse(null);
        if (current == null || current.state() != TableControlState.RENAMING
                || current.metadataRevision() != expected.metadataRevision()) {
            throw new AccessDeniedException("RENAME completed with a changed control record for "
                    + expected.qualifiedName() + "; reconciliation is required");
        }
        TableIdentity physical = identity.filter(TableIdentity::hasFormatUuid)
                .filter(value -> value.format().equals(current.format()))
                .orElseThrow(() -> new AccessDeniedException(
                        "RENAME completed without verifiable format identity for "
                                + expected.qualifiedName() + "; reconciliation is required"));
        catalogs.requireCurrent(current, "RENAME completion");
        if ("ICEBERG".equalsIgnoreCase(current.format())
                && !physical.formatTableUuid().equals(current.formatTableUuid())) {
            throw new AccessDeniedException("RENAME completed with a different Iceberg table UUID for "
                    + expected.qualifiedName() + "; reconciliation is required");
        }
        Instant now = clock.instant();
        String token = version.map(TableVersionToken::token).orElse(null);
        TableControlRecord completed = TablePhysicalIdentity.withIdentity(current, Optional.of(physical), version, now)
                .state(TableControlState.ACTIVE)
                .properties(TableControlRenameIntent.complete(current))
                .version(token, token == null ? null : now)
                .metadataRevision(current.metadataRevision() + 1)
                .build();
        if (token == null) {
            store.save(completed);
        } else {
            store.saveWithObservation(completed, new TableVersionObservation(current.tableId(),
                    token, current.versionToken(), "DDL_RENAME", now));
        }
    }
}
