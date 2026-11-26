package org.pangrova.planner.substrait;

import org.pangrova.planner.exception.SubstraitConversionException;

import org.pangrova.protocol.substrait.SubstraitProtocolVersion;

import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import org.pangrova.common.enums.PangrovaErrorCode;

/** Serializes official Substrait plans without a second private plan decoder. */
public final class SubstraitPlanSerializer {

    private final PlanProtoConverter protoConverter = new PlanProtoConverter(
            new PangrovaExtensionProtoConverter());

    /** Serializes a Substrait plan and stamps the canonical producer version. */
    public byte[] serialize(Plan plan) {
        return toProto(plan).toByteArray();
    }

    /** Builds the canonical producer message for live in-process plan binding. */
    public io.substrait.proto.Plan toProto(Plan plan) {
        try {
            io.substrait.proto.Plan.Builder builder = SubstraitProtoNormalizer
                    .normalize(protoConverter.toProto(plan))
                    .toBuilder();
            SubstraitProtocolVersion.stamp(builder);
            return builder.build();
        } catch (Exception e) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Failed to serialize Substrait Plan: " + e.getMessage(), e);
        }
    }
}
