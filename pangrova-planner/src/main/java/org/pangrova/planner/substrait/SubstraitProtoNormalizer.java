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
        return builder.build();
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
