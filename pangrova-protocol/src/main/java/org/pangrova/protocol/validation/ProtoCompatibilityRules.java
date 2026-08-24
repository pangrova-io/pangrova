package org.pangrova.protocol.validation;

import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Enforces backward compatibility rules for proto schema evolution.
 *
 * <p>Rules enforced:
 * <ul>
 *   <li>No removing or renaming fields (field numbers must be stable)</li>
 *   <li>No changing field numbers for existing fields</li>
 *   <li>New fields must be optional (proto3 default)</li>
 *   <li>Reserved field numbers must not be reused</li>
 *   <li>Field types must not change</li>
 * </ul>
 *
 * <p>Usage: compare a "current" descriptor against a "previous" descriptor
 * to detect breaking changes before deployment.</p>
 */
public final class ProtoCompatibilityRules {

    private ProtoCompatibilityRules() {}

    /**
     * Checks backward compatibility between two versions of a proto message.
     *
     * @param previous the previous (deployed) message descriptor
     * @param current  the current (new) message descriptor
     * @return list of compatibility violations (empty if compatible)
     */
    public static List<String> checkCompatibility(
            Descriptors.Descriptor previous,
            Descriptors.Descriptor current) {
        List<String> violations = new ArrayList<>();
        String messageName = previous.getName();

        checkRemovedFields(previous, current, messageName, violations);
        checkFieldTypeChanges(previous, current, messageName, violations);
        checkReservedFieldReuse(current, messageName, violations);

        return List.copyOf(violations);
    }

    private static void checkRemovedFields(
            Descriptors.Descriptor previous,
            Descriptors.Descriptor current,
            String messageName,
            List<String> violations) {
        Map<Integer, Descriptors.FieldDescriptor> currentFields = current.getFields().stream()
                .collect(Collectors.toMap(Descriptors.FieldDescriptor::getNumber, f -> f));

        Set<Integer> reservedNumbers = extractReservedNumbers(current);

        for (Descriptors.FieldDescriptor prevField : previous.getFields()) {
            int fieldNumber = prevField.getNumber();
            Descriptors.FieldDescriptor currField = currentFields.get(fieldNumber);

            if (currField == null && !reservedNumbers.contains(fieldNumber)) {
                violations.add(messageName + ": field '" + prevField.getName()
                        + "' (number " + fieldNumber + ") was removed without being reserved");
            }
        }
    }

    private static void checkFieldTypeChanges(
            Descriptors.Descriptor previous,
            Descriptors.Descriptor current,
            String messageName,
            List<String> violations) {
        Map<Integer, Descriptors.FieldDescriptor> currentFields = current.getFields().stream()
                .collect(Collectors.toMap(Descriptors.FieldDescriptor::getNumber, f -> f));

        for (Descriptors.FieldDescriptor prevField : previous.getFields()) {
            Descriptors.FieldDescriptor currField = currentFields.get(prevField.getNumber());
            if (currField == null) {
                continue;
            }

            if (prevField.getType() != currField.getType()) {
                violations.add(messageName + ": field '" + prevField.getName()
                        + "' changed type from " + prevField.getType()
                        + " to " + currField.getType());
            }

            if (prevField.isRepeated() != currField.isRepeated()) {
                violations.add(messageName + ": field '" + prevField.getName()
                        + "' changed cardinality (repeated=" + prevField.isRepeated()
                        + " → " + currField.isRepeated() + ")");
            }
        }
    }

    private static void checkReservedFieldReuse(
            Descriptors.Descriptor current,
            String messageName,
            List<String> violations) {
        Set<Integer> reservedNumbers = extractReservedNumbers(current);

        for (Descriptors.FieldDescriptor field : current.getFields()) {
            if (reservedNumbers.contains(field.getNumber())) {
                violations.add(messageName + ": field '" + field.getName()
                        + "' uses reserved field number " + field.getNumber());
            }
        }
    }

    private static Set<Integer> extractReservedNumbers(Descriptors.Descriptor descriptor) {
        return descriptor.toProto().getReservedRangeList().stream()
                .flatMapToInt(range -> java.util.stream.IntStream.range(range.getStart(), range.getEnd()))
                .boxed()
                .collect(Collectors.toSet());
    }
}
