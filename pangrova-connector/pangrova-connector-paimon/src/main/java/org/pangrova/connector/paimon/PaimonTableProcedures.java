package org.pangrova.connector.paimon;

import org.pangrova.connector.api.metadata.TableProcedure;

import java.util.List;

/**
 * Enumerates the table maintenance procedures supported by the Paimon connector.
 *
 * <p>Returned by {@link PaimonMetadata#listTableProcedures()} for connector
 * discovery. The descriptors do not grant SQL CALL admission; the coordinator
 * applies its maintenance-policy contract independently.</p>
 */
final class PaimonTableProcedures {

    private PaimonTableProcedures() {}

    private static final List<TableProcedure> PROCEDURES = List.of(
            procedure("compact",
                    "Triggers full compaction on a Paimon table, merging small files and "
                            + "materializing deletions into clean data files.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                            "Schema-qualified table name (schema.table)"))),
            procedure("expire_snapshots",
                    "Expires old snapshots using Paimon's ExpireSnapshots API, cleaning "
                            + "up data and manifest files for expired snapshots while preserving "
                            + "tagged snapshots. Supports dry_run.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.optional("retain_max",
                                    "Maximum number of snapshots to retain", "30"),
                            TableProcedure.PropertyDefinition.optional("retain_min",
                                    "Minimum number of snapshots to retain", "10"),
                            TableProcedure.PropertyDefinition.optional("dry_run",
                                    "Report what would expire without committing", "false"))),
            procedure("remove_orphan_files",
                    "Removes orphan files not referenced by any snapshot using Paimon's "
                            + "LocalOrphanFilesClean. The cutoff must be at least 24 hours old; "
                            + "supports dry_run.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.optional("older_than_ms",
                                    "Only delete files older than this epoch (ms)", null),
                            TableProcedure.PropertyDefinition.optional("dry_run",
                                    "Report orphans without deleting them", "false"))),
            procedure("create_tag",
                    "Creates a named tag on the latest snapshot.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.required("tag_name",
                                    "The tag name to create"),
                            TableProcedure.PropertyDefinition.optional("snapshot_id",
                                    "Snapshot ID to tag; defaults to the latest", null),
                            TableProcedure.PropertyDefinition.optional("time_retained",
                                    "Tag retention duration, for example 1 d", null))),
            procedure("delete_tag",
                    "Deletes a named tag.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.required("tag_name",
                                    "The tag name to delete"))),
            procedure("rollback_to",
                    "Rolls back the table to a previously tagged snapshot.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.required("tag_name",
                                    "The tag to roll back to"))),
            procedure("create_branch",
                    "Creates a named branch from the current table state.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.required("branch_name",
                                    "The branch name to create"))),
            procedure("delete_branch",
                    "Deletes a named branch.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.required("branch_name",
                                    "The branch name to delete")))
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
