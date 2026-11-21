package org.pangrova.worker.task;

import org.pangrova.protocol.v2.TaskKind;

/**
 * Maps REST taskType string to canonical v2 TaskKind.
 * Exhaustive switch (no default) ensures compile-time safety on new task types.
 *
 * <p>Mapping table (see docs/architecture/jni-runtime.md §1):
 * <pre>
 *   COMPUTE → TASK_KIND_READ
 *   OUTPUT  → TASK_KIND_READ
 *   WRITE               → TASK_KIND_WRITE
 *   BACKGROUND_BUILD    → TASK_KIND_BACKGROUND_BUILD
 *   BACKGROUND_VALIDATE → TASK_KIND_BACKGROUND_VALIDATE
 *   BACKGROUND_EXPORT   → TASK_KIND_BACKGROUND_EXPORT
 * </pre>
 */
public final class TaskTypeMapper {

    private TaskTypeMapper() {}

    /**
     * Converts REST taskType string to Protobuf TaskType enum.
     *
     * @param restTaskType a supported control-plane task type
     * @return Protobuf TaskType enum value
     * @throws IllegalArgumentException if restTaskType is unknown
     */
    public static TaskKind toProtoType(String restTaskType) {
        return switch (restTaskType) {
            case "COMPUTE" -> TaskKind.TASK_KIND_READ;
            case "OUTPUT" -> TaskKind.TASK_KIND_READ;
            case "WRITE" -> TaskKind.TASK_KIND_WRITE;
            case "BACKGROUND_BUILD" -> TaskKind.TASK_KIND_BACKGROUND_BUILD;
            case "BACKGROUND_VALIDATE" -> TaskKind.TASK_KIND_BACKGROUND_VALIDATE;
            case "BACKGROUND_EXPORT" -> TaskKind.TASK_KIND_BACKGROUND_EXPORT;
            default -> throw new IllegalArgumentException(
                    "Unknown REST taskType: " + restTaskType);
        };
    }

    /**
     * Converts REST taskType string to Protobuf TaskType string name.
     * Used for logging and REST DTO serialization.
     *
     * @param restTaskType a supported control-plane task type
     * @return Protobuf TaskType string name
     */
    public static String toProtoTypeName(String restTaskType) {
        return toProtoType(restTaskType).name();
    }
}
