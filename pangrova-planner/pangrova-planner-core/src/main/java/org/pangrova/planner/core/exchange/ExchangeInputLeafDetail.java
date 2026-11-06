package org.pangrova.planner.core.exchange;

import com.google.protobuf.Any;
import io.substrait.extension.ExtensionCollector;
import io.substrait.relation.Extension;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.proto.TypeProtoConverter;
import org.pangrova.planner.core.cost.distribution.PangrovaExchangeInput;
import org.pangrova.protocol.v2.ExchangeDistribution;
import org.pangrova.protocol.v2.ExchangeInputDetail;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

/** Serializes a fragmented exchange consumer as a canonical v2 extension leaf. */
public final class ExchangeInputLeafDetail implements Extension.LeafRelDetail {

    private final ExchangeInputDetail detail;
    private final Type.Struct recordType;

    /** Binds the exact exchange identity and its typed consumer schema. */
    public ExchangeInputLeafDetail(PangrovaExchangeInput input, NamedStruct schema) {
        this.recordType = schema.struct();
        this.detail = ExchangeInputDetail.newBuilder()
                .setPlanNodeId(input.planNodeId())
                .setExchangeId(input.exchangeId())
                .setUpstreamStageId(input.upstreamStageId())
                .setDownstreamStageId(input.downstreamStageId())
                .setDistribution(mapDistribution(input))
                .setPartitionCount(input.partitionCount())
                .setOutputSchema(schema.toProto(
                        new TypeProtoConverter(new ExtensionCollector())))
                .setBatchSerde(input.batchSerde())
                .setRequireComplete(input.requireComplete())
                .build();
    }

    private static ExchangeDistribution mapDistribution(PangrovaExchangeInput input) {
        return switch (input.distribution().type()) {
            case HASH -> ExchangeDistribution.EXCHANGE_DISTRIBUTION_HASH;
            case BROADCAST -> ExchangeDistribution.EXCHANGE_DISTRIBUTION_BROADCAST;
            case ROUND_ROBIN -> ExchangeDistribution.EXCHANGE_DISTRIBUTION_ROUND_ROBIN;
            case SINGLETON -> ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON;
            case ANY -> throw new IllegalArgumentException(
                    "ANY distribution cannot be serialized as an exchange input");
        };
    }

    @Override
    public Type.Struct deriveRecordType() {
        return recordType;
    }

    @Override
    public Any toProto(RelProtoConverter converter) {
        RelationDetailEnvelope envelope = RelationDetailEnvelope.newBuilder()
                .setSchemaVersion(1)
                .setExchangeInput(detail)
                .build();
        return Any.pack(envelope);
    }
}
