package org.pangrova.governance.api.quality;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Pre-commit validation gate that evaluates data quality rules before
 * INSERT/UPDATE operations are committed.
 *
 * <p>Implementations check all enabled quality rules for the target table
 * and block the write if any rule with blocking severity fails.</p>
 */
@Experimental
public interface WriteQualityGate {

    /**
     * Validate quality rules for the target table before commit.
     *
     * @param catalogName catalog containing the target table
     * @param schemaName  schema containing the target table
     * @param tableName   target table name
     * @return validation result indicating whether the write may proceed
     */
    WriteValidationResult validate(String catalogName, String schemaName, String tableName);
}
