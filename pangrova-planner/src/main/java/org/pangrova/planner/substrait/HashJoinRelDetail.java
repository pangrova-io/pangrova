package org.pangrova.planner.substrait;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.substrait.relation.Extension;
import io.substrait.relation.Rel;
import io.substrait.type.Type;

import java.util.List;

/**
 * Encodes physical hash/broadcast join metadata for the native engine.
 *
 * <p>Serialized as a Substrait ExtensionMultiRel detail. The native
 * engine parses the JSON payload to build the appropriate join node
 * (HashJoinNode or broadcast-based HashJoinNode).</p>
 *
 * @param strategy the join strategy: "hash_join" or "broadcast_join"
 * @param joinType the join type (INNER, LEFT, RIGHT, OUTER)
 * @param leftKeys join key column indices from the left input
 * @param rightKeys join key column indices from the right input
 * @param outputType the output record type of the join
 */
public record HashJoinRelDetail(
        String strategy,
        String joinType,
        List<Integer> leftKeys,
        List<Integer> rightKeys,
        Type.Struct outputType) implements Extension.MultiRelDetail {

    @Override
    public Type.Struct deriveRecordType(List<Rel> inputs) {
        return outputType;
    }

    @Override
    public Any toProto() {
        var json = new StringBuilder("{");
        json.append("\"op\":\"").append(strategy).append("\"");
        json.append(",\"joinType\":\"").append(joinType).append("\"");
        json.append(",\"leftKeys\":[");
        for (int i = 0; i < leftKeys.size(); i++) {
            if (i > 0) json.append(",");
            json.append(leftKeys.get(i));
        }
        json.append("]");
        json.append(",\"rightKeys\":[");
        for (int i = 0; i < rightKeys.size(); i++) {
            if (i > 0) json.append(",");
            json.append(rightKeys.get(i));
        }
        json.append("]");
        json.append("}");
        return Any.pack(StringValue.of(json.toString()));
    }
}
