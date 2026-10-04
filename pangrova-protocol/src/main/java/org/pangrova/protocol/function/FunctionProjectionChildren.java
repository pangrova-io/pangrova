package org.pangrova.protocol.function;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.substrait.proto.Type;
import org.pangrova.protocol.v2.FunctionTypeProjectionChildPath;

import java.util.ArrayList;
import java.util.List;

/** The sole descriptor traversal for independently projected canonical children. */
final class FunctionProjectionChildren {
    private FunctionProjectionChildren() {}

    static void validate(Type root, FunctionTypeProjectionChildPath path) {
        requirePath(path);
        Descriptor descriptor = root.getDescriptorForType();
        for (int position = 0; position < path.getFieldNumbersCount(); position++) {
            var field = field(descriptor, path.getFieldNumbers(position), position + 1 == path.getFieldNumbersCount());
            descriptor = field.getMessageType();
        }
    }

    static List<Type> read(Type root, FunctionTypeProjectionChildPath path) {
        validate(root, path);
        Message current = root;
        for (int position = 0; position < path.getFieldNumbersCount(); position++) {
            boolean terminal = position + 1 == path.getFieldNumbersCount();
            var field = field(current.getDescriptorForType(), path.getFieldNumbers(position), terminal);
            if (terminal) return children(current, field);
            if (!current.hasField(field)) throw new IllegalArgumentException("Canonical recursive type path is absent");
            current = (Message) current.getField(field);
        }
        throw new IllegalArgumentException("Canonical recursive type path is empty");
    }

    static Type replace(Type root, FunctionTypeProjectionChildPath path, List<Type> values) {
        validate(root, path);
        return (Type) replace(root, path, values, 0);
    }

    private static Message replace(Message root, FunctionTypeProjectionChildPath path,
                                   List<Type> values, int position) {
        boolean terminal = position + 1 == path.getFieldNumbersCount();
        var field = field(root.getDescriptorForType(), path.getFieldNumbers(position), terminal);
        var builder = root.toBuilder();
        if (!terminal) {
            var child = (Message) root.getField(field);
            builder.setField(field, replace(child, path, values, position + 1));
        } else if (field.isRepeated()) {
            builder.clearField(field);
            values.forEach(value -> builder.addRepeatedField(field, value));
        } else {
            if (values.size() != 1) {
                throw new IllegalArgumentException("Canonical singular child must have one projected value");
            }
            builder.setField(field, values.get(0));
        }
        return builder.build();
    }

    private static List<Type> children(Message root, FieldDescriptor field) {
        if (!field.isRepeated()) {
            if (!root.hasField(field)) throw new IllegalArgumentException("Canonical recursive child is absent");
            return List.of((Type) root.getField(field));
        }
        List<Type> result = new ArrayList<>();
        for (int index = 0; index < root.getRepeatedFieldCount(field); index++) {
            result.add((Type) root.getRepeatedField(field, index));
        }
        return List.copyOf(result);
    }

    private static FieldDescriptor field(Descriptor descriptor, int number, boolean terminal) {
        var field = number > 0 ? descriptor.findFieldByNumber(number) : null;
        if (field == null || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                || (!terminal && field.isRepeated())
                || (terminal && !field.getMessageType().equals(Type.getDescriptor()))) {
            throw new IllegalArgumentException("Recursive projection is not an exact canonical Type child path");
        }
        return field;
    }

    private static void requirePath(FunctionTypeProjectionChildPath path) {
        if (!path.getUnknownFields().asMap().isEmpty() || path.getFieldNumbersCount() == 0
                || path.getFieldNumbersCount() > 64) {
            throw new IllegalArgumentException("Canonical recursive projection path is unknown, empty or too deep");
        }
    }
}
