package org.pangrova.connector.paimon;

import org.pangrova.spi.connector.metadata.TableProcedure;

import java.util.List;

/**
 * Enumerates the table maintenance procedures supported by the Paimon connector.
 *
 * <p>Returned by {@link PaimonMetadata#listTableProcedures()} so the engine can
 * expose available procedures in information-schema queries and SQL autocompletion.</p>
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
                            + "tagged snapshots.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.optional("retain_max",
                                    "Maximum number of snapshots to retain", "1"))),
            procedure("remove_orphan_files",
                    "Removes orphan files not referenced by any snapshot using Paimon's "
                            + "LocalOrphanFilesClean.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.optional("older_than_ms",
                                    "Only delete files older than this epoch (ms)", null))),
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
                                    "The branch name to delete"))),
            procedure("merge_branch",
                    "Merges a branch into the main branch.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)"),
                            TableProcedure.PropertyDefinition.required("branch_name",
                                    "The branch to merge"))),
            procedure("repair",
                    "Validates snapshot chain integrity and reports gaps.",
                    List.of(TableProcedure.PropertyDefinition.required("table_name",
                                    "Schema-qualified table name (schema.table)")))
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
