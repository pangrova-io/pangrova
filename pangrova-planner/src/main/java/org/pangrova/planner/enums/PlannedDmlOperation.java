package org.pangrova.planner.enums;

/**
 * DML operation type extracted from the compiled plan.
 */
public enum PlannedDmlOperation {
    INSERT,
    DELETE,
    UPDATE,
    MERGE;

    /**
     * Parses the Calcite TableModify.Operation name into a PlannedDmlOperation.
     */
    public static PlannedDmlOperation fromCalciteName(String name) {
        return switch (name) {
            case "INSERT" -> INSERT;
            case "DELETE" -> DELETE;
            case "UPDATE" -> UPDATE;
            case "MERGE" -> MERGE;
            default -> throw new IllegalArgumentException(
                    "Unknown DML operation: " + name);
        };
    }
}
