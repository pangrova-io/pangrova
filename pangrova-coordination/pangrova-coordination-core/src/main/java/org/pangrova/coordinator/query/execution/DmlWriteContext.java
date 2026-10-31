package org.pangrova.coordinator.query.execution;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Carries DML operation metadata through the dispatch chain so that
 * {@code QueryDispatcher} can build a WRITE task request
 * with the correct {@code WriteConfig} for native workers.
 *
 * <p>Extracted from {@code LogicalTableModify} before Substrait conversion
 * strips the DML operation type from the plan.</p>
 *
 * @param targetTable structured catalog, schema and table identity
 * @param writeMode   canonical DML mode: INSERT, OVERWRITE, UPDATE, DELETE, or MERGE
 * @param partitionSpec canonical static partition bindings encoded as {@code name=value}
 */
public record DmlWriteContext(
        org.pangrova.connector.spi.metadata.table.QualifiedTableName targetTable,
        String writeMode,
        java.util.List<String> partitionSpec,
        java.util.Optional<WritePolicyObservation> policyObservation) {

    public DmlWriteContext {
        checkNotNull(targetTable, "targetTable");
        for (String component : new String[] {targetTable.catalog(), targetTable.schema(), targetTable.table()}) {
            if (component == null || component.isBlank()) {
                throw new IllegalArgumentException("Distributed write target requires nonblank catalog.schema.table");
            }
        }
        checkNotNull(writeMode, "writeMode");
        switch (writeMode) {
            case "INSERT", "OVERWRITE", "UPDATE", "DELETE", "MERGE" -> { }
            default -> throw new IllegalArgumentException(
                    "Unsupported distributed write mode: " + writeMode);
        }
        partitionSpec = java.util.List.copyOf(partitionSpec);
        policyObservation = java.util.Objects.requireNonNull(policyObservation, "policyObservation");
    }
}
