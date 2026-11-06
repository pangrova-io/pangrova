package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.system.PaimonAuditLogSystemTable;
import org.pangrova.connector.paimon.system.PaimonBranchesSystemTable;
import org.pangrova.connector.paimon.system.PaimonConsumersSystemTable;
import org.pangrova.connector.paimon.system.PaimonManifestsSystemTable;
import org.pangrova.connector.paimon.system.PaimonOptionsSystemTable;
import org.pangrova.connector.paimon.system.PaimonSchemasSystemTable;
import org.pangrova.connector.paimon.system.PaimonSnapshotsSystemTable;
import org.pangrova.connector.paimon.system.PaimonTagsSystemTable;
import org.pangrova.connector.spi.metadata.ConnectorSystemTable;
import org.apache.paimon.table.Table;

import java.util.Optional;

/**
 * Resolves Paimon system tables (snapshots, schemas, files, ...) by name.
 */
final class PaimonSystemTables {

    private PaimonSystemTables() {}

    /** Resolves the named system table for the given Paimon table, or empty when unsupported. */
    static Optional<ConnectorSystemTable> resolve(Table paimonTable, String systemTableName) {
        return switch (systemTableName) {
            case "snapshots" -> Optional.of(new PaimonSnapshotsSystemTable(paimonTable));
            case "schemas" -> Optional.of(new PaimonSchemasSystemTable(paimonTable));
            case "options" -> Optional.of(new PaimonOptionsSystemTable(paimonTable));
            case "tags" -> Optional.of(new PaimonTagsSystemTable(paimonTable));
            case "branches" -> Optional.of(new PaimonBranchesSystemTable(paimonTable));
            case "consumers" -> Optional.of(new PaimonConsumersSystemTable(paimonTable));
            case "audit_log" -> Optional.of(new PaimonAuditLogSystemTable(paimonTable));
            case "manifests" -> Optional.of(new PaimonManifestsSystemTable(paimonTable));
            default -> Optional.empty();
        };
    }
}
