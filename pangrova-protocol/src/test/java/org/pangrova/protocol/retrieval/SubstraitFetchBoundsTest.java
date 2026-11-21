package org.pangrova.protocol.retrieval;

import io.substrait.proto.Expression;
import io.substrait.proto.FetchRel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubstraitFetchBoundsTest {
    @Test
    void canonicalLiteralBoundsAreExactAndAbsenceOnlyDefaultsTheOffset() {
        var fetch = FetchRel.newBuilder().setCountExpr(SubstraitFetchBounds.literal(Long.MAX_VALUE)).build();
        assertThat(SubstraitFetchBounds.count(fetch)).isEqualTo(Long.MAX_VALUE);
        assertThat(SubstraitFetchBounds.offset(fetch)).isZero();
        assertThat(SubstraitFetchBounds.count(FetchRel.getDefaultInstance())).isEqualTo(-1);
        assertThat(SubstraitFetchBounds.offset(fetch.toBuilder()
                .setOffsetExpr(SubstraitFetchBounds.literal(3)).build())).isEqualTo(3);
    }

    @Test
    @SuppressWarnings("deprecation")
    void invalidAndDeprecatedBoundsNeverMatchACanonicalTopK() {
        assertThat(SubstraitFetchBounds.count(FetchRel.newBuilder().setCount(10).build())).isEqualTo(-1);
        assertThat(SubstraitFetchBounds.offset(FetchRel.newBuilder().setOffset(0).build())).isEqualTo(-1);
        var negative = Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setI64(-2));
        assertThat(SubstraitFetchBounds.count(FetchRel.newBuilder().setCountExpr(negative).build())).isEqualTo(-1);
        assertThat(SubstraitFetchBounds.offset(FetchRel.newBuilder().setOffsetExpr(negative).build())).isEqualTo(-1);
        var narrowed = Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setI32(10));
        assertThat(SubstraitFetchBounds.count(FetchRel.newBuilder().setCountExpr(narrowed).build())).isEqualTo(-1);
        assertThatThrownBy(() -> SubstraitFetchBounds.literal(-1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("nonnegative");
    }
}
