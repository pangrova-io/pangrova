package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.metadata.TableProcedure;

import java.util.List;

/**
 * Enumerates the table maintenance procedures supported by the Iceberg connector.
 *
 * <p>Returned by {@link IcebergMetadata#listTableProcedures()} so the engine can
 * expose available procedures in information-schema queries and SQL autocompletion.</p>
 *
 * <p>Provides Trino/Spark-style {@code CALL catalog.procedure_name(args)} parity
 * for Iceberg tables. All procedures dispatch to real Iceberg SDK APIs via
 * {@link IcebergMaintenanceProcedures}.</p>
 */
final class IcebergTableProcedures {

    private IcebergTableProcedures() {}

    private static final List<TableProcedure> PROCEDURES = List.of(
            procedure("compact",
                    "Rewrites data files that have associated delete files (position deletes "
                            + "or deletion vectors), materializing deletions into clean data files "
                            + "via Iceberg RewriteFiles.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                            "Schema-qualified table name (schema.table)"))),
            procedure("expire_snapshots",
                    "Atomically expires old snapshots and cleans orphaned data/manifest files "
                            + "using Iceberg's ExpireSnapshots API. Supports dry_run.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.optional("retain_last",
                                    "Number of latest snapshots to retain", "10"),
                            TableProcedure.PropertyDefinition.optional("older_than_millis",
                                    "Expire snapshots older than this epoch (ms)", null),
                            TableProcedure.PropertyDefinition.optional("dry_run",
                                    "Report what would expire without committing", "false"))),
            procedure("remove_orphan_files",
                    "Walks the table location and deletes files not reachable from any "
                            + "snapshot or manifest. Requires a FileIO that supports prefix listing. "
                            + "The cutoff must be at least 24 hours old; supports dry_run.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.optional("older_than_millis",
                                    "Only delete files older than this epoch (ms)", null),
                            TableProcedure.PropertyDefinition.optional("dry_run",
                                    "Report orphans without deleting them", "false"))),
            procedure("rewrite_manifests",
                    "Compacts small manifest files into larger ones for better planning "
                            + "performance.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                            "Schema-qualified table name (schema.table)"))),
            procedure("rollback_to_snapshot",
                    "Rolls the current branch reference back to a previous snapshot ID.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.required("snapshot_id",
                                    "The snapshot ID to roll back to"))),
            procedure("set_current_snapshot",
                    "Fast-forwards or sets the main branch to a specific snapshot ID.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.required("snapshot_id",
                                    "The snapshot ID to set as current"))),
            procedure("cherrypick_snapshot",
                    "Applies a staged or WAP snapshot's changes onto the current branch.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.required("snapshot_id",
                                    "The snapshot ID to cherry-pick")))
    );

    static List<TableProcedure> list() {
        return PROCEDURES;
    }

    private static TableProcedure procedure(
            String name, String description,
            List<TableProcedure.PropertyDefinition> properties) {
        return new TableProcedure() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public String description() {
                return description;
            }

            @Override
            public List<PropertyDefinition> properties() {
                return properties;
            }
        };
    }
}
