package org.pangrova.planner.core.extension;

import com.google.protobuf.Any;
import io.substrait.expression.ExpressionCreator;
import io.substrait.relation.Extension;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityInvocation;
import org.pangrova.protocol.v2.NamedExpressionArgument;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.TableFunctionDetail;

import java.util.List;

/** Zero-input Native series source with one declared BIGINT output column. */
public record GenerateSeriesRelDetail(String planNodeId, List<Long> arguments,
                                      NamedStruct outputSchema) implements Extension.LeafRelDetail {
    public GenerateSeriesRelDetail {
        arguments = List.copyOf(arguments);
        if (arguments.size() != 2 && arguments.size() != 3) {
            throw new IllegalArgumentException("GENERATE_SERIES requires 2 or 3 arguments");
        }
        if (arguments.size() == 3 && arguments.get(2) == 0) {
            throw new IllegalArgumentException("GENERATE_SERIES: step must not be zero");
        }
    }

    @Override
    public Type.Struct deriveRecordType() {
        return outputSchema.struct();
    }

    @Override
    public Any toProto(RelProtoConverter converter) {
        var invocation = CapabilityInvocation.newBuilder()
                .setCapability(BuiltInCapabilityCatalog.generateSeriesPlanRef());
        String[] names = {"start", "stop", "step"};
        for (int i = 0; i < arguments.size(); i++) {
            invocation.addArguments(NamedExpressionArgument.newBuilder().setName(names[i])
                    .setExpression(converter.getExpressionProtoConverter()
                            .toProto(ExpressionCreator.i64(false, arguments.get(i)))));
        }
        var detail = TableFunctionDetail.newBuilder().setPlanNodeId(planNodeId)
                .setInvocation(invocation)
                .setOutputSchema(outputSchema.toProto(converter.getTypeProtoConverter()));
        return Any.pack(RelationDetailEnvelope.newBuilder().setSchemaVersion(1)
                .setTableFunction(detail).build());
    }
}
