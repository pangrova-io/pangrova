package org.pangrova.protocol.substrait;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Rel;
import io.substrait.proto.Type;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import java.util.List;

/** Only the shared typed envelope owns extension output metadata. */
final class SubstraitExtensionShape {
    private SubstraitExtensionShape() {}

    static NamedStruct describe(Rel relation, List<NamedStruct> inputs) {
        Any detail = relation.hasExtensionLeaf() ? relation.getExtensionLeaf().getDetail()
                : relation.hasExtensionSingle() ? relation.getExtensionSingle().getDetail()
                : relation.getExtensionMulti().getDetail();
        RelationDetailEnvelope envelope;
        try {
            envelope = detail.unpack(RelationDetailEnvelope.class);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Relation requires the canonical typed detail envelope", e);
        }
        if (envelope.getSchemaVersion() != 1) throw new IllegalArgumentException("Unknown relation envelope version");
        NamedStruct schema = switch (envelope.getDetailCase()) {
            case RETRIEVAL_SOURCE -> envelope.getRetrievalSource().getOutputContract().getSchema();
            case RETRIEVAL_MERGE -> envelope.getRetrievalMerge().getOutputContract().getSchema();
            case RERANK -> envelope.getRerank().getOutputContract().getSchema();
            case CANDIDATE_FILL -> envelope.getCandidateFill().getOutputSchema();
            case AI_INVOKE -> envelope.getAiInvoke().getOutputSchema();
            case EXCHANGE_INPUT -> envelope.getExchangeInput().getOutputSchema();
            case TABLE_FUNCTION -> envelope.getTableFunction().getOutputSchema();
            case ARTIFACT_BUILD -> SubstraitRelationShapes.concatenate(List.of());
            case ENFORCE_SINGLE_ROW -> enforceSingleRow(
                    relation, inputs, envelope.getEnforceSingleRow().getInputOrdinal());
            default -> throw new IllegalArgumentException("Relation envelope has no output contract");
        };
        SubstraitSchemaFields.fields(schema).forEach(field -> SubstraitExpressionTypes.declared(field.type()));
        return schema;
    }

    private static NamedStruct enforceSingleRow(Rel relation, List<NamedStruct> inputs, int ordinal) {
        if (!relation.hasExtensionSingle() || inputs.size() != 1) {
            throw new IllegalArgumentException("Single-row enforcement requires one complete input");
        }
        var input = inputs.get(0);
        var fields = SubstraitSchemaFields.fields(input);
        if (ordinal < 0 || ordinal >= fields.size()) {
            throw new IllegalArgumentException("Single-row value is outside the input schema");
        }
        var value = fields.get(ordinal);
        var nullable = SubstraitSchemaFields.withNullability(value.type(), Type.Nullability.NULLABILITY_NULLABLE);
        return SubstraitSchemaFields.schema(List.of(new SubstraitSchemaFields.Field(
                value.name(), nullable, value.descendants())), input.getStruct());
    }
}
