package org.pangrova.metadata.core.control;

import org.pangrova.metadata.api.control.OwnershipMode;
import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
import org.pangrova.spi.security.AccessDeniedException;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

/**
 * Shared invariants for the table control plane: identity evidence, catalog
 * deployment fencing, record construction, and pending-rename guards.
 *
 * <p>These helpers are implementation details of {@code core.control} and are
 * never exported outside this package.</p>
 */
final class TableControlSupport {

    static final String LEGACY_FORMAT = "UNKNOWN";

    private TableControlSupport() {}

    static boolean matchesIdentity(TableControlRecord record, Optional<TableIdentity> identity) {
        if (identity.isEmpty() || !identity.get().hasFormatUuid()) {
            return true;
        }
        if (!hasFormatUuid(record)) {
            return true;
        }
        return sameIdentity(record, identity.get());
    }

    static boolean sameIdentity(TableControlRecord record, TableIdentity identity) {
        return record.format().equals(identity.format())
                && record.formatTableUuid().equals(identity.formatTableUuid())
                && java.util.Objects.equals(record.formatLocation(), identity.location());
    }

    static boolean hasFormatUuid(TableControlRecord record) {
        return record.formatTableUuid() != null && !record.formatTableUuid().isBlank();
    }

    static TableControlRecord.Builder withIdentity(TableControlRecord.Builder builder,
            Optional<TableIdentity> identity, Optional<TableVersionToken> version, Instant now) {
        identity.ifPresent(value -> builder
                .format(value.format())
                .formatTableUuid(value.formatTableUuid())
                .formatLocation(value.location())
                .sourceSignature(sourceSignature(value)));
        if (version.isPresent()) {
            builder.version(version.get().token(), now);
        }
        return builder;
    }

    static TableControlRecord.Builder withIdentity(TableControlRecord record,
            Optional<TableIdentity> identity, Optional<TableVersionToken> version, Instant now) {
        return withIdentity(record.toBuilder().updatedAt(now), identity, version, now);
    }

    static String sourceSignature(TableIdentity identity) {
        return identity.format() + '|' + identity.formatTableUuid() + '|' + identity.location();
    }

    /** Rejects authority captured for another deployment, even if the format UUID is unchanged. */
    static void requireCurrentCatalog(Function<String, CatalogDeploymentIdentity> catalogs,
            TableControlRecord record, String operation) {
        if (!matchesCatalog(record, catalogs.apply(record.catalog()))) {
            throw new AccessDeniedException(operation + " rejected: catalog deployment identity changed or "
                    + "was never registered for " + record.qualifiedName()
                    + "; explicitly reconcile the control record before using its authority");
        }
    }

    static boolean matchesCatalog(TableControlRecord record, CatalogDeploymentIdentity deployment) {
        return deployment.incarnationId().equals(record.catalogIncarnationId())
                && java.util.Objects.equals(deployment.revision(), record.catalogRevision());
    }

    static void requireNoPendingRename(TableControlRecord record) {
        if (record.state() == TableControlState.RENAMING) {
            throw new AccessDeniedException("CREATE rejected: name is reserved by pending rename " + record.tableId());
        }
    }

    static TableControlRecord.Builder newRecord(Function<String, CatalogDeploymentIdentity> catalogs,
            String catalog, String schema, String table, String ownerPrincipal,
            TableControlProvenance provenance, Instant now) {
        CatalogDeploymentIdentity deployment = catalogs.apply(catalog);
        return TableControlRecord.builder()
                .tableId(UUID.randomUUID())
                .name(catalog, schema, table)
                .catalogIncarnationId(deployment.incarnationId())
                .catalogRevision(deployment.revision())
                .format(LEGACY_FORMAT)
                .ownershipMode(OwnershipMode.MANAGED)
                .state(TableControlState.ACTIVE)
                .provenance(provenance)
                .metadataRevision(1)
                .ownerPrincipal(ownerPrincipal)
                .createdAt(now)
                .updatedAt(now);
    }
}
