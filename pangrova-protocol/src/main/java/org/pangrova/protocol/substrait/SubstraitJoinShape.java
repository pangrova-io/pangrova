package org.pangrova.protocol.substrait;

import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Message;
import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Rel;
import io.substrait.proto.Type;

import java.util.ArrayList;
import java.util.List;

/** Join enums are identified by canonical names, not their differing wire numbers. */
final class SubstraitJoinShape {
    private SubstraitJoinShape() {}

    static NamedStruct describe(Rel relation, List<NamedStruct> inputs, NamedStruct context) {
        if (inputs.size() != 2) throw new IllegalArgumentException("Join requires two inputs");
        Message join = SubstraitRelationLayout.payload(relation);
        validateCondition(join, "expression", context);
        String kind = ((EnumValueDescriptor) join.getField(
                join.getDescriptorForType().findFieldByName("type"))).getName();
        NamedStruct output = shape(kind, inputs);
        validateCondition(join, "post_join_filter", output);
        return output;
    }

    private static NamedStruct shape(String kind, List<NamedStruct> inputs) {
        var left = SubstraitSchemaFields.fields(inputs.get(0));
        var right = SubstraitSchemaFields.fields(inputs.get(1));
        if (kind.endsWith("_SEMI") || kind.endsWith("_ANTI")) {
            return SubstraitSchemaFields.schema(kind.contains("LEFT") ? left : right);
        }
        if (kind.endsWith("_MARK")) {
            var fields = new ArrayList<>(kind.contains("LEFT") ? left : right);
            fields.add(SubstraitSchemaFields.computed("join_mark",
                    SubstraitSchemaFields.primitive("bool", Type.Nullability.NULLABILITY_NULLABLE)));
            return SubstraitSchemaFields.schema(fields);
        }
        return joined(kind, left, right);
    }

    private static NamedStruct joined(String kind, List<SubstraitSchemaFields.Field> left,
                                     List<SubstraitSchemaFields.Field> right) {
        boolean nullableLeft = kind.equals("JOIN_TYPE_OUTER") || kind.equals("JOIN_TYPE_RIGHT")
                || kind.equals("JOIN_TYPE_RIGHT_SINGLE");
        boolean nullableRight = kind.equals("JOIN_TYPE_OUTER") || kind.equals("JOIN_TYPE_LEFT")
                || kind.equals("JOIN_TYPE_LEFT_SINGLE");
        if (!nullableLeft && !nullableRight && !kind.equals("JOIN_TYPE_INNER")) {
            throw new IllegalArgumentException("Join requires a canonical join type: " + kind);
        }
        List<SubstraitSchemaFields.Field> result = new ArrayList<>();
        result.addAll(nullableLeft ? nullable(left) : left);
        result.addAll(nullableRight ? nullable(right) : right);
        return SubstraitSchemaFields.schema(result);
    }

    private static List<SubstraitSchemaFields.Field> nullable(List<SubstraitSchemaFields.Field> fields) {
        return fields.stream().map(field -> new SubstraitSchemaFields.Field(field.name(),
                SubstraitSchemaFields.withNullability(field.type(), Type.Nullability.NULLABILITY_NULLABLE),
                field.descendants())).toList();
    }

    private static void validateCondition(Message message, String name, NamedStruct context) {
        var field = message.getDescriptorForType().findFieldByName(name);
        if (field != null && message.hasField(field)) {
            SubstraitRelationShapes.requireBoolean((Expression) message.getField(field), context);
        }
    }
}
