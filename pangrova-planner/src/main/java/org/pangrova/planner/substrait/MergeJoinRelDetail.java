package org.pangrova.planner.substrait;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.substrait.relation.Extension;
import io.substrait.relation.Rel;
import io.substrait.type.Type;

import java.util.List;

/**
 * Encodes MergeJoin metadata for the native engine.
 *
 * <p>Serialized as a Substrait ExtensionMultiRel detail. The native
 * engine parses the JSON payload to build a native MergeJoinNode.</p>
 *
 * <p>The hint includes whether inputs are pre-sorted on join keys.
 * When inputs are not pre-sorted, the native engine inserts SortNode
 * operators before the MergeJoinNode.</p>
 *
 * @param joinType the join type (INNER, LEFT, RIGHT, OUTER)
 * @param leftKeys join key column indices from the left input
 * @param rightKeys join key column indices from the right input
 * @param inputsPreSorted whether both inputs are already sorted on join keys
 * @param outputType the output record type of the merge join
 */
public record MergeJoinRelDetail(
        String joinType,
        List<Integer> leftKeys,
        List<Integer> rightKeys,
        boolean inputsPreSorted,
        Type.Struct outputType) implements Extension.MultiRelDetail {

    @Override
    public Type.Struct deriveRecordType(List<Rel> inputs) {
        return outputType;
    }

    @Override
    public Any toProto() {
        var json = new StringBuilder("{");
        json.append("\"op\":\"merge_join\"");
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
        json.append(",\"inputsPreSorted\":").append(inputsPreSorted);
        json.append("}");
        return Any.pack(StringValue.of(json.toString()));
    }
}
