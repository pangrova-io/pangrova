package org.pangrova.planner.substrait;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.substrait.relation.Extension;
import io.substrait.relation.Rel;
import io.substrait.type.Type;

import java.util.List;

/**
 * Encodes nested loop join metadata for the native engine.
 *
 * <p>Serialized as a Substrait ExtensionMultiRel detail. The native
 * engine parses the JSON payload to build a NestedLoopJoinNode that
 * evaluates the join condition row-by-row across all input pairs.</p>
 *
 * @param joinType the join type (INNER, LEFT, RIGHT, OUTER, SEMI, ANTI)
 * @param outputType the output record type of the nested loop join
 */
public record NestedLoopJoinRelDetail(
        String joinType,
        Type.Struct outputType) implements Extension.MultiRelDetail {

    @Override
    public Type.Struct deriveRecordType(List<Rel> inputs) {
        return outputType;
    }

    @Override
    public Any toProto() {
        var json = new StringBuilder("{");
        json.append("\"op\":\"nested_loop_join\"");
        json.append(",\"joinType\":\"").append(joinType).append("\"");
        json.append("}");
        return Any.pack(StringValue.of(json.toString()));
    }
}
