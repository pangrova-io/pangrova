package org.pangrova.protocol.validation;

import org.pangrova.protocol.exception.ProtoValidationException;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Validates protobuf messages at deserialization time, enforcing field
 * constraints (required fields, value ranges, string length limits).
 *
 * <p>Proto3 does not have native "required" semantics, so this validator
 * checks that critical fields are populated (non-default) and that values
 * fall within acceptable ranges.</p>
 */
public final class ProtoValidator {

    /** Maximum allowed length for identifier fields (*_id). */
    private static final int MAX_ID_FIELD_LENGTH = 256;
    /** Maximum allowed length for path fields (*_path). */
    private static final int MAX_PATH_FIELD_LENGTH = 4096;
    /** Default maximum allowed string length (64KB). */
    private static final int DEFAULT_MAX_STRING_LENGTH = 65536;

    /** Exact field names whose values must be non-negative. */
    private static final Set<String> NON_NEGATIVE_FIELD_NAMES = Set.of(
            "length", "start_offset", "max_threads", "generation", "num_partitions", "batch_size");
    /** Field-name suffixes whose values must be non-negative. */
    private static final List<String> NON_NEGATIVE_FIELD_SUFFIXES = List.of(
            "_bytes", "_count", "_ms", "_nanos");
    /** Presence rules are explicit; optional identifiers must never become required by naming. */
    private static final Set<String> REQUIRED_FIELDS = Set.of(
            "pangrova.protocol.v2.TaskAttemptId.query_or_job_id",
            "pangrova.protocol.v2.TaskAttemptId.task_id",
            "pangrova.protocol.v2.DataSplit.split_id",
            "pangrova.protocol.v2.DataSplit.connector_id",
            "pangrova.protocol.v2.DataSplit.opaque_location",
            "pangrova.protocol.v2.TaskRequest.substrait_plan");

    private ProtoValidator() {}

    /**
     * Validates a proto message against field constraints.
     * Throws {@link ProtoValidationException} if any constraint is violated.
     *
     * @param message the proto message to validate
     * @throws ProtoValidationException if validation fails
     */
    public static void validate(Message message) {
        List<String> violations = collectViolations(message);
        if (!violations.isEmpty()) {
            throw new ProtoValidationException(message.getDescriptorForType().getName(), violations);
        }
    }

    /**
     * Validates a proto message and returns violations without throwing.
     *
     * @param message the proto message to validate
     * @return list of violation descriptions (empty if valid)
     */
    public static List<String> collectViolations(Message message) {
        List<String> violations = new ArrayList<>();
        validateMessage(message, "", violations);
        return List.copyOf(violations);
    }

    private static void validateMessage(Message message, String path, List<String> violations) {
        Descriptors.Descriptor descriptor = message.getDescriptorForType();
        String messageName = descriptor.getName();

        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            String fieldPath = path.isEmpty()
                    ? messageName + "." + field.getName()
                    : path + "." + field.getName();

            if (field.isRepeated()) {
                if (!field.isMapField()
                        && field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    @SuppressWarnings("unchecked")
                    List<Message> values = (List<Message>) message.getField(field);
                    for (int index = 0; index < values.size(); index++) {
                        validateMessage(values.get(index), fieldPath + "[" + index + "]", violations);
                    }
                }
                continue;
            }

            validateField(message, field, fieldPath, violations);
        }
    }

    private static void validateField(Message message, Descriptors.FieldDescriptor field,
                                      String fieldPath, List<String> violations) {
        switch (field.getType()) {
            case STRING -> validateStringField(message, field, fieldPath, violations);
            case INT32, SINT32 -> validateInt32Field(message, field, fieldPath, violations);
            case INT64, SINT64 -> validateInt64Field(message, field, fieldPath, violations);
            case BYTES -> validateBytesField(message, field, fieldPath, violations);
            case MESSAGE -> {
                if (message.hasField(field)) {
                    validateMessage((Message) message.getField(field), fieldPath, violations);
                }
            }
            default -> { /* enums and other types: no additional validation */ }
        }
    }

    private static void validateStringField(Message message, Descriptors.FieldDescriptor field,
                                            String fieldPath, List<String> violations) {
        String value = (String) message.getField(field);
        if (isRequiredField(field) && value.isEmpty()) {
            violations.add(fieldPath + ": required string field is empty");
        }
        if (!value.isEmpty() && value.length() > getMaxStringLength(field)) {
            violations.add(fieldPath + ": string length " + value.length()
                    + " exceeds maximum " + getMaxStringLength(field));
        }
    }

    private static void validateInt32Field(Message message, Descriptors.FieldDescriptor field,
                                           String fieldPath, List<String> violations) {
        int value = (int) message.getField(field);
        if (isNonNegativeField(field) && value < 0) {
            violations.add(fieldPath + ": value " + value + " must be non-negative");
        }
    }

    private static void validateInt64Field(Message message, Descriptors.FieldDescriptor field,
                                           String fieldPath, List<String> violations) {
        long value = (long) message.getField(field);
        if (isNonNegativeField(field) && value < 0) {
            violations.add(fieldPath + ": value " + value + " must be non-negative");
        }
    }

    private static void validateBytesField(Message message, Descriptors.FieldDescriptor field,
                                           String fieldPath, List<String> violations) {
        com.google.protobuf.ByteString value = (com.google.protobuf.ByteString) message.getField(field);
        if (isRequiredField(field) && value.isEmpty()) {
            violations.add(fieldPath + ": required bytes field is empty");
        }
    }

    /**
     * Determines whether a scalar is required at the canonical boundary.
     * The list is explicit because many {@code *_id} values are optional
     * references; name-based inference made valid v2 messages impossible.
     */
    private static boolean isRequiredField(Descriptors.FieldDescriptor field) {
        return REQUIRED_FIELDS.contains(
                field.getContainingType().getFullName() + "." + field.getName());
    }

    /**
     * Determines if a numeric field must be non-negative based on naming conventions.
     * Fields representing sizes, counts, offsets, or durations must be non-negative.
     */
    private static boolean isNonNegativeField(Descriptors.FieldDescriptor field) {
        String name = field.getName();
        return NON_NEGATIVE_FIELD_NAMES.contains(name)
                || NON_NEGATIVE_FIELD_SUFFIXES.stream().anyMatch(name::endsWith);
    }

    /**
     * Returns the maximum allowed string length for a field.
     * Default is 64KB; ID fields are limited to 256 chars.
     */
    private static int getMaxStringLength(Descriptors.FieldDescriptor field) {
        String name = field.getName();
        if (name.endsWith("_id")) {
            return MAX_ID_FIELD_LENGTH;
        }
        if (name.endsWith("_path")) {
            return MAX_PATH_FIELD_LENGTH;
        }
        return DEFAULT_MAX_STRING_LENGTH;
    }
}
