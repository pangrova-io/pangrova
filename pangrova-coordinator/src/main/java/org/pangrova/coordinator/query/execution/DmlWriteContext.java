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
 * @param writeMode   DML write mode string for WriteConfig (DELETE, UPDATE, MERGE)
 */
public record DmlWriteContext(String targetTable, String writeMode) {

    public DmlWriteContext {
        checkNotNull(targetTable, "targetTable");
        checkNotNull(writeMode, "writeMode");
    }
}
