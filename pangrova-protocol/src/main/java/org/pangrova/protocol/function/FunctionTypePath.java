package org.pangrova.protocol.function;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.substrait.proto.Type;
import org.pangrova.protocol.v2.FunctionTypePathStep;

import java.util.List;

/** Descriptor-authorized paths in canonical Type messages; no type-name mapping. */
final class FunctionTypePath {
    record Leaf(Message message, FieldDescriptor field, Integer index) {
        Object value() {
            return index == null ? message.getField(field) : message.getRepeatedField(field, index);
        }
    }

    private FunctionTypePath() {}

    static FieldDescriptor kind(Type type) {
        var oneof = Type.getDescriptor().getOneofs().stream().filter(value -> value.getName().equals("kind"))
                .findFirst().orElseThrow(() -> new IllegalArgumentException(
                        "Canonical Type descriptor has no kind oneof"));
        var field = type.getOneofFieldDescriptor(oneof);
        if (field == null) throw new IllegalArgumentException("Canonical Type has no declared kind");
        return field;
    }

    static Leaf read(Message root, List<FunctionTypePathStep> path) {
        requirePath(path);
        Message current = root;
        for (int position = 0; position < path.size(); position++) {
            var leaf = leaf(current, path.get(position));
            if (position == path.size() - 1) return leaf;
            current = child(leaf);
        }
        throw new IllegalArgumentException("Canonical type path is empty");
    }

    static Message replace(Message root, List<FunctionTypePathStep> path, Object value) {
        requirePath(path);
        return replace(root, path, value, 0);
    }

    private static Message replace(Message root, List<FunctionTypePathStep> path, Object value, int position) {
        var leaf = leaf(root, path.get(position));
        Object replacement = position + 1 == path.size() ? value
                : replace(child(leaf), path, value, position + 1);
        var builder = root.toBuilder();
        if (leaf.index() == null) builder.setField(leaf.field(), replacement);
        else builder.setRepeatedField(leaf.field(), leaf.index(), replacement);
        return builder.build();
    }

    private static Leaf leaf(Message message, FunctionTypePathStep step) {
        if (!step.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Canonical type path contains unknown fields");
        }
        var field = message.getDescriptorForType().findFieldByNumber(step.getFieldNumber());
        if (field == null || field.isRepeated() != step.hasRepeatedIndex()) {
            throw new IllegalArgumentException("Canonical type path has an unknown field or wrong cardinality");
        }
        Integer index = step.hasRepeatedIndex() ? step.getRepeatedIndex() : null;
        if (index != null && (index < 0 || index >= message.getRepeatedFieldCount(field))) {
            throw new IllegalArgumentException("Canonical type path repeated index is out of range");
        }
        if (!field.isRepeated() && field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                && !message.hasField(field)) {
            throw new IllegalArgumentException("Canonical type path traverses an absent message");
        }
        return new Leaf(message, field, index);
    }

    private static Message child(Leaf leaf) {
        if (leaf.field().getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            throw new IllegalArgumentException("Canonical type path traverses a scalar field");
        }
        return (Message) leaf.value();
    }

    private static void requirePath(List<FunctionTypePathStep> path) {
        if (path == null || path.isEmpty() || path.size() > 64 || path.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException("Canonical type path must be explicit and bounded");
        }
    }
}
