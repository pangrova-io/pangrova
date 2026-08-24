package org.pangrova.planner.substrait;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.substrait.relation.Extension;
import io.substrait.relation.Rel;
import io.substrait.type.Type;

import java.util.List;
import java.util.Objects;

/**
 * Detail for encoding a physical Exchange (data redistribution) operator
 * in the Substrait plan as an ExtensionSingleRel.
 *
 * <p>The Substrait spec defines ExchangeRel at the proto level, but the
 * substrait-java SDK (0.23.0) does not expose a high-level relation model
 * for it. We use ExtensionSingleRel with a structured detail payload to
 * encode exchange semantics, matching the pattern used for physical joins
 * (HashJoinRelDetail, MergeJoinRelDetail).</p>
 *
 * <p>The native engine parses the detail JSON to build the appropriate
 * shuffle operator (hash partition, broadcast, round-robin, etc.).</p>
 *
 * @param distributionType the exchange strategy (HASH, BROADCAST, SINGLETON, ROUND_ROBIN)
 * @param hashKeys         column indices for HASH distribution (empty for others)
 * @param outputType       the output struct type
 */
public record ExchangeRelDetail(
        String distributionType,
        List<Integer> hashKeys,
        Type.Struct outputType) implements Extension.SingleRelDetail {

    public ExchangeRelDetail {
        Objects.requireNonNull(distributionType, "distributionType must not be null");
        Objects.requireNonNull(hashKeys, "hashKeys must not be null");
        Objects.requireNonNull(outputType, "outputType must not be null");
        hashKeys = List.copyOf(hashKeys);
    }

    @Override
    public Type.Struct deriveRecordType(Rel input) {
        return outputType;
    }

    @Override
    public Any toProto() {
        var json = new StringBuilder("{");
        json.append("\"op\":\"exchange\"");
        json.append(",\"distributionType\":\"").append(distributionType).append("\"");
        if (!hashKeys.isEmpty()) {
            json.append(",\"hashKeys\":[");
            for (int i = 0; i < hashKeys.size(); i++) {
                if (i > 0) json.append(",");
                json.append(hashKeys.get(i));
            }
            json.append("]");
        }
        json.append("}");
        return Any.pack(StringValue.of(json.toString()));
    }
}
