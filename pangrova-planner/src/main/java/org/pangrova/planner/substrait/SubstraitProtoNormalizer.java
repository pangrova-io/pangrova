package org.pangrova.planner.substrait;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.substrait.proto.Expression;
import io.substrait.proto.FetchRel;
import io.substrait.proto.HashJoinRel;
import io.substrait.proto.MergeJoinRel;
import io.substrait.proto.Plan;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Normalizes official SDK output to the current Substrait wire contract. */
final class SubstraitProtoNormalizer {

    private static final Set<Descriptor> JOIN_TYPES =
            Set.of(HashJoinRel.getDescriptor(), MergeJoinRel.getDescriptor());
    private static final Map<Descriptor, List<FieldDescriptor>> RELATION_FIELDS = relationFields();

    private SubstraitProtoNormalizer() {}

    static Plan normalize(Plan plan) {
        return (Plan) normalizeMessage(plan);
    }

    private static Message normalizeMessage(Message message) {
        Descriptor type = message.getDescriptorForType();
        Message.Builder changed = type == FetchRel.getDescriptor() ? normalizeFetch(message) : null;
        for (var field : RELATION_FIELDS.getOrDefault(type, List.of())) {
            changed = normalizeField(message, changed, field);
        }
        if (JOIN_TYPES.contains(type)) {
            var left = type.findFieldByName("left_keys");
            var right = type.findFieldByName("right_keys");
            if (message.getRepeatedFieldCount(left) != 0 || message.getRepeatedFieldCount(right) != 0) {
                if (changed == null) changed = message.toBuilder();
                normalizeJoinKeys(changed, left, right);
            }
        }
        return changed == null ? message : changed.build();
    }

    private static Map<Descriptor, List<FieldDescriptor>> relationFields() {
        // Index the descriptor graph once. The reverse walk includes relation
        // children inside subqueries and excludes types, literals and metadata.
        var visited = new HashSet<Descriptor>();
        var queue = new ArrayDeque<Descriptor>();
        var parents = new HashMap<Descriptor, List<FieldDescriptor>>();
        queue.add(Plan.getDescriptor());
        while (!queue.isEmpty()) {
            var type = queue.remove();
            if (!visited.add(type)) continue;
            for (var field : type.getFields()) {
                if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    var child = field.getMessageType();
                    parents.computeIfAbsent(child, ignored -> new ArrayList<>()).add(field);
                    queue.add(child);
                }
            }
        }

        var fields = new HashMap<Descriptor, List<FieldDescriptor>>();
        visited.clear();
        queue.add(FetchRel.getDescriptor());
        queue.addAll(JOIN_TYPES);
        while (!queue.isEmpty()) {
            var child = queue.remove();
            if (!visited.add(child)) continue;
            for (var field : parents.getOrDefault(child, List.of())) {
                var containing = field.getContainingType();
                fields.computeIfAbsent(containing, ignored -> new ArrayList<>()).add(field);
                queue.add(containing);
            }
        }
        fields.replaceAll((type, children) -> children.stream()
                .sorted(Comparator.comparingInt(FieldDescriptor::getNumber)).toList());
        return Map.copyOf(fields);
    }

    private static void normalizeJoinKeys(
            Message.Builder join, FieldDescriptor leftField, FieldDescriptor rightField) {
        var keysField = join.getDescriptorForType().findFieldByName("keys");
        int legacyLeft = join.getRepeatedFieldCount(leftField);
        int legacyRight = join.getRepeatedFieldCount(rightField);
        int canonical = join.getRepeatedFieldCount(keysField);
        if (canonical == 0 || legacyLeft != canonical || legacyRight != canonical) {
            throw new IllegalArgumentException("Deprecated join keys differ from canonical keys");
        }
        for (int ordinal = 0; ordinal < canonical; ordinal++) {
            Message key = (Message) join.getRepeatedField(keysField, ordinal);
            var keyDescriptor = key.getDescriptorForType();
            Message comparison = (Message) key.getField(
                    keyDescriptor.findFieldByName("comparison"));
            var simple = comparison.getDescriptorForType().findFieldByName("simple");
            if (!comparison.hasField(simple)
                    || ((EnumValueDescriptor) comparison.getField(simple)).getNumber() != 1) {
                throw new IllegalArgumentException("Deprecated join keys require canonical EQ comparison");
            }
            if (!key.getField(keyDescriptor.findFieldByName("left"))
                    .equals(join.getRepeatedField(leftField, ordinal))
                    || !key.getField(keyDescriptor.findFieldByName("right"))
                    .equals(join.getRepeatedField(rightField, ordinal))) {
                throw new IllegalArgumentException("Deprecated join keys differ from canonical keys");
            }
        }
        join.clearField(leftField);
        join.clearField(rightField);
    }

    private static Message.Builder normalizeField(
            Message original, Message.Builder changed, FieldDescriptor field) {
        if (field.isRepeated()) {
            for (int index = 0; index < original.getRepeatedFieldCount(field); index++) {
                var child = (Message) original.getRepeatedField(field, index);
                var normalized = normalizeMessage(child);
                if (normalized != child) {
                    if (changed == null) changed = original.toBuilder();
                    changed.setRepeatedField(field, index, normalized);
                }
            }
        } else if (original.hasField(field)) {
            var child = (Message) original.getField(field);
            var normalized = normalizeMessage(child);
            if (normalized != child) {
                if (changed == null) changed = original.toBuilder();
                changed.setField(field, normalized);
            }
        }
        return changed;
    }

    private static Message.Builder normalizeFetch(Message fetch) {
        var descriptor = fetch.getDescriptorForType();
        var changed = integerToExpression(fetch, null,
                descriptor.findFieldByName("offset"), descriptor.findFieldByName("offset_expr"));
        return integerToExpression(fetch, changed,
                descriptor.findFieldByName("count"), descriptor.findFieldByName("count_expr"));
    }

    private static Message.Builder integerToExpression(Message fetch, Message.Builder changed,
            FieldDescriptor legacy, FieldDescriptor replacement) {
        if (legacy == null || replacement == null || !fetch.hasField(legacy)) return changed;
        long value = (Long) fetch.getField(legacy);
        if (value < 0) {
            throw new IllegalArgumentException("Fetch value must be non-negative: " + value);
        }
        if (fetch.hasField(replacement)) return changed;
        if (changed == null) changed = fetch.toBuilder();
        changed.setField(replacement, Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder().setI64(value)).build());
        return changed;
    }
}
