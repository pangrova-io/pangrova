package org.pangrova.worker.task;

import org.pangrova.protocol.v1.TaskType;

/**
 * Maps REST taskType string to Protobuf TaskType enum.
 * Exhaustive switch (no default) ensures compile-time safety on new task types.
 *
 * <p>Mapping table (see docs/architecture/20-java-native-protocol.md §4):
 * <pre>
 *   COMPUTE → TASK_TYPE_READ
 *   OUTPUT  → TASK_TYPE_READ
 *   WRITE   → TASK_TYPE_WRITE
 * </pre>
 */
public final class TaskTypeMapper {

    private TaskTypeMapper() {}

    /**
     * Converts REST taskType string to Protobuf TaskType enum.
     *
     * @param restTaskType one of "COMPUTE", "OUTPUT", "WRITE"
     * @return Protobuf TaskType enum value
     * @throws IllegalArgumentException if restTaskType is unknown
     */
    public static TaskType toProtoType(String restTaskType) {
        return switch (restTaskType) {
            case "COMPUTE" -> TaskType.TASK_TYPE_READ;
            case "OUTPUT" -> TaskType.TASK_TYPE_READ;
            case "WRITE" -> TaskType.TASK_TYPE_WRITE;
            default -> throw new IllegalArgumentException(
                    "Unknown REST taskType: " + restTaskType);
        };
    }

    /**
     * Converts REST taskType string to Protobuf TaskType string name.
     * Used for logging and REST DTO serialization.
     *
     * @param restTaskType one of "COMPUTE", "OUTPUT", "WRITE"
     * @return Protobuf TaskType string name
     */
    public static String toProtoTypeName(String restTaskType) {
        return toProtoType(restTaskType).name();
    }
}
