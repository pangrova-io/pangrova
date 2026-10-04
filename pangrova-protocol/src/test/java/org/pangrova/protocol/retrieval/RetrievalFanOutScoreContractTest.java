package org.pangrova.protocol.retrieval;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.ScoreDirection;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetrievalFanOutScoreContractTest {
    @Test
    void exactSourcesAcceptExplicitNullableAndRequiredScoreContracts() {
        for (var nullable : List.of(Type.Nullability.NULLABILITY_REQUIRED, Type.Nullability.NULLABILITY_NULLABLE)) {
            var score = Type.newBuilder().setFp64(Type.FP64.newBuilder().setNullability(nullable)).build();
            assertThatCode(() -> RetrievalFanOutScoreContract.validate(sources(score, 1, false))).doesNotThrowAnyException();
        }
    }

    @Test
    void bm25RequiresItsActualNullableScoreContract() {
        assertThatCode(() -> RetrievalFanOutScoreContract.validate(
                sources(Bm25MaterializedScoreBinder.nullableFp64(), 1, true))).doesNotThrowAnyException();
        var required = Type.newBuilder().setFp64(Type.FP64.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
        assertThatThrownBy(() -> RetrievalFanOutScoreContract.validate(sources(required, 1, true)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit compatible nullability");
    }

    @Test
    void unspecifiedNonFloatingAndOutOfRangeScoresRejectBeforeRewriting() {
        var unspecified = Type.newBuilder().setFp64(Type.FP64.newBuilder()).build();
        var integer = Type.newBuilder().setI64(Type.I64.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
        for (var score : List.of(unspecified, integer)) {
            assertThatThrownBy(() -> RetrievalFanOutScoreContract.validate(sources(score, 1, false)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit compatible nullability");
        }
        assertThatThrownBy(() -> RetrievalFanOutScoreContract.validate(
                sources(Bm25MaterializedScoreBinder.nullableFp64(), 2, false)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ordinal is out of bounds");
    }

    private static RetrievalFanOutBinding sources(Type score, int ordinal, boolean bm25) {
        var schema = NamedStruct.newBuilder().addNames("id").addNames("score")
                .setStruct(Type.Struct.newBuilder().addTypes(Type.newBuilder().setI64(Type.I64.newBuilder()
                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED))).addTypes(score)).build();
        var first = RetrievalTopKBinding.exact(ordinal, 10, ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                List.of(0), schema.toByteArray());
        var second = bm25 ? RetrievalTopKBinding.artifact(ordinal, 10,
                ScoreDirection.SCORE_DIRECTION_HIGHER_IS_BETTER, List.of(0), schema.toByteArray(),
                new RetrievalTopKBinding.ArtifactInput("table", BuiltInCapabilityCatalog.bm25RetrievalPlanRef(),
                        List.of(), ArtifactBinding.getDefaultInstance(), "profile", 10, 1.0, 10)) : first;
        return new RetrievalFanOutBinding(List.of(new RetrievalFanOutBinding.Source("first", first),
                new RetrievalFanOutBinding.Source("second", second)));
    }
}
