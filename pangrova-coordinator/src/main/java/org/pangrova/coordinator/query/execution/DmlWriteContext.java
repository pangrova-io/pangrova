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
 * @param targetTable fully qualified target table name (catalog.schema.table)
 * @param writeMode   canonical DML mode: INSERT, OVERWRITE, UPDATE, DELETE, or MERGE
 * @param partitionSpec canonical static partition bindings encoded as {@code name=value}
 */
public record DmlWriteContext(
        String targetTable,
        String writeMode,
        java.util.List<String> partitionSpec,
        java.util.Optional<WritePolicyObservation> policyObservation) {

    public DmlWriteContext {
        checkNotNull(targetTable, "targetTable");
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
