package org.pangrova.planner.substrait;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.substrait.relation.Extension;
import io.substrait.relation.Rel;
import io.substrait.type.Type;

import java.util.List;

/**
 * Encodes UNNEST (EXPLODE/POSEXPLODE) metadata for the native engine.
 *
 * <p>Serialized as a Substrait ExtensionSingleRel detail. The native
 * engine parses the JSON payload to build a native UnnestNode.</p>
 *
 * @param unnestColumnIndices indices of ARRAY/MAP columns to unnest (from left input)
 * @param unnestOutputNames output column names for unnested elements
 * @param withOrdinality whether to include a position index column (POSEXPLODE)
 * @param outputType the output record type of the unnest operation
 */
record UnnestRelDetail(
        List<Integer> unnestColumnIndices,
        List<String> unnestOutputNames,
        boolean withOrdinality,
        Type.Struct outputType) implements Extension.SingleRelDetail {

    @Override
    public Type.Struct deriveRecordType(Rel input) {
        return outputType;
    }

    @Override
    public Any toProto() {
        var json = new StringBuilder("{");
        json.append("\"op\":\"unnest\"");
        json.append(",\"unnestColumns\":[");
        for (int i = 0; i < unnestColumnIndices.size(); i++) {
            if (i > 0) json.append(",");
            json.append(unnestColumnIndices.get(i));
        }
        json.append("]");
        json.append(",\"unnestNames\":[");
        for (int i = 0; i < unnestOutputNames.size(); i++) {
            if (i > 0) json.append(",");
            json.append("\"").append(unnestOutputNames.get(i)).append("\"");
        }
        json.append("]");
        json.append(",\"ordinality\":").append(withOrdinality);
        json.append("}");
        return Any.pack(StringValue.of(json.toString()));
    }
}
