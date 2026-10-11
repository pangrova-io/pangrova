package org.pangrova.planner.extension;

import com.google.protobuf.Any;
import io.substrait.expression.FieldReference;
import io.substrait.relation.Extension;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityInvocation;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.NamedExpressionArgument;
import org.pangrova.protocol.v2.NamedValue;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.TableFunctionDetail;
import org.pangrova.protocol.v2.TypedValue;

/**
 * One structural native UNNEST, with a single canonical typed envelope.
 */
public record UnnestRelDetail(String planNodeId, int collectionIndex, boolean withOrdinality,
                       boolean outer, NamedStruct outputSchema) implements Extension.SingleRelDetail {

    @Override
    public Type.Struct deriveRecordType(Rel input) {
        return outputSchema.struct();
    }

    @Override
    public Any toProto(RelProtoConverter converter) {
        var collection = FieldReference.newRootStructReference(
                collectionIndex, outputSchema.struct().fields().get(collectionIndex));
        var invocation = CapabilityInvocation.newBuilder()
                .setCapability(BuiltInCapabilityCatalog.unnestPlanRef())
                .addArguments(NamedExpressionArgument.newBuilder().setName("input_0")
                        .setExpression(converter.getExpressionProtoConverter().toProto(collection)))
                .addOptions(option("with_ordinality", withOrdinality, converter))
                .addOptions(option("outer", outer, converter));
        var detail = TableFunctionDetail.newBuilder().setPlanNodeId(planNodeId)
                .setInvocation(invocation)
                .setOutputSchema(outputSchema.toProto(converter.getTypeProtoConverter()));
        return Any.pack(RelationDetailEnvelope.newBuilder().setSchemaVersion(1)
                .setTableFunction(detail).build());
    }

    private static NamedValue option(String name, boolean value, RelProtoConverter converter) {
        return NamedValue.newBuilder().setName(name).setValue(TypedValue.newBuilder()
                .setBoolValue(value).setType(LogicalType.newBuilder().setCanonicalSql("BOOLEAN")
                        .setSubstraitType(converter.getTypeProtoConverter()
                                .toProto(TypeCreator.REQUIRED.BOOLEAN)))).build();
    }
}
