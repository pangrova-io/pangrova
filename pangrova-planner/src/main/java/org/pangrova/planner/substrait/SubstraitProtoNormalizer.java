package org.pangrova.planner.substrait;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.substrait.proto.Plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Normalizes official SDK output to the current Substrait wire contract. */
final class SubstraitProtoNormalizer {

    private static final String FETCH_REL = "substrait.FetchRel";
    private static final String MERGE_JOIN_REL = "substrait.MergeJoinRel";
    private static final String HASH_JOIN_REL = "substrait.HashJoinRel";

    private SubstraitProtoNormalizer() {}

    static Plan normalize(Plan plan) {
        return (Plan) normalizeMessage(plan);
    }

    private static Message normalizeMessage(Message message) {
        Message.Builder builder = message.toBuilder();
        if (FETCH_REL.equals(message.getDescriptorForType().getFullName())) {
            normalizeFetch(builder);
        }
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry
                : message.getAllFields().entrySet()) {
            normalizeField(builder, entry.getKey(), entry.getValue());
        }
        if (MERGE_JOIN_REL.equals(message.getDescriptorForType().getFullName())
                || HASH_JOIN_REL.equals(message.getDescriptorForType().getFullName())) {
            normalizeJoinKeys(builder);
        }
        return builder.build();
    }

    private static void normalizeJoinKeys(Message.Builder join) {
        var descriptor = join.getDescriptorForType();
        var keysField = descriptor.findFieldByName("keys");
        var leftField = descriptor.findFieldByName("left_keys");
        var rightField = descriptor.findFieldByName("right_keys");
        int legacyLeft = join.getRepeatedFieldCount(leftField);
        int legacyRight = join.getRepeatedFieldCount(rightField);
        if (legacyLeft == 0 && legacyRight == 0) return;
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
                    || ((Descriptors.EnumValueDescriptor) comparison.getField(simple)).getNumber() != 1) {
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

    private static void normalizeField(
            Message.Builder builder,
            Descriptors.FieldDescriptor field,
            Object value) {
        if (field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return;
        }
        if (field.isRepeated()) {
            List<?> values = new ArrayList<>((List<?>) value);
            for (int index = 0; index < values.size(); index++) {
                builder.setRepeatedField(
                        field, index, normalizeMessage((Message) values.get(index)));
            }
            return;
        }
        builder.setField(field, normalizeMessage((Message) value));
    }

    private static void normalizeFetch(Message.Builder fetch) {
        Descriptors.Descriptor descriptor = fetch.getDescriptorForType();
        moveIntegerToExpression(fetch,
                descriptor.findFieldByName("offset"),
                descriptor.findFieldByName("offset_expr"));
        moveIntegerToExpression(fetch,
                descriptor.findFieldByName("count"),
                descriptor.findFieldByName("count_expr"));
    }

    private static void moveIntegerToExpression(
            Message.Builder fetch,
            Descriptors.FieldDescriptor oldField,
            Descriptors.FieldDescriptor expressionField) {
        if (oldField == null || expressionField == null || !fetch.hasField(oldField)) {
            return;
        }
        long value = (Long) fetch.getField(oldField);
        if (value < 0) {
            throw new IllegalArgumentException(
                    "Fetch value must be non-negative: " + value);
        }
        Message.Builder expression = fetch.newBuilderForField(expressionField);
        Descriptors.FieldDescriptor literalField =
                expression.getDescriptorForType().findFieldByName("literal");
        Message.Builder literal = expression.newBuilderForField(literalField);
        Descriptors.FieldDescriptor integerField =
                literal.getDescriptorForType().findFieldByName("i64");
        literal.setField(integerField, value);
        expression.setField(literalField, literal.build());
        fetch.setField(expressionField, expression.build());
    }
}
