package org.pangrova.protocol.retrieval;

import org.pangrova.protocol.constants.BuiltInCapabilityIdentifiers;

import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;

/** Validates declared score types before independently bound branches share an exchange. */
final class RetrievalFanOutScoreContract {
    private RetrievalFanOutScoreContract() {}

    static void validate(RetrievalFanOutBinding fanOut) throws InvalidProtocolBufferException {
        boolean bm25 = fanOut.sources().stream().anyMatch(source ->
                BuiltInCapabilityIdentifiers.BM25_RETRIEVAL_ID.equals(
                        source.binding().capability().getCapabilityId()));
        for (var source : fanOut.sources()) {
            var binding = source.binding();
            var schema = NamedStruct.parseFrom(binding.candidateSchema());
            int ordinal = binding.scoreFieldIndex();
            if (ordinal >= schema.getStruct().getTypesCount()) {
                throw new IllegalArgumentException("Fan-out score ordinal is out of bounds");
            }
            var score = schema.getStruct().getTypes(ordinal);
            if (!score.hasFp64() || score.getFp64().getNullability()
                    == Type.Nullability.NULLABILITY_UNSPECIFIED
                    || bm25 && !score.equals(Bm25MaterializedScoreBinder.nullableFp64())) {
                throw new IllegalArgumentException(
                        "Fan-out score requires an explicit compatible nullability contract");
            }
        }
    }
}
