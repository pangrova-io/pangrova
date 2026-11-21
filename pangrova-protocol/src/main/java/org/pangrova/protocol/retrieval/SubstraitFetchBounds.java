package org.pangrova.protocol.retrieval;

import io.substrait.proto.Expression;
import io.substrait.proto.FetchRel;

/** Canonical literal bounds shared by the retrieval plan rewrites. */
final class SubstraitFetchBounds {
    private SubstraitFetchBounds() {}

    static long count(FetchRel fetch) {
        return fetch.hasCountExpr() ? value(fetch.getCountExpr()) : -1;
    }

    static long offset(FetchRel fetch) {
        return switch (fetch.getOffsetModeCase()) {
            case OFFSET_EXPR -> value(fetch.getOffsetExpr());
            case OFFSETMODE_NOT_SET -> 0;
            default -> -1;
        };
    }

    static Expression literal(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Fetch bound must be nonnegative");
        }
        return Expression.newBuilder().setLiteral(
                Expression.Literal.newBuilder().setI64(value)).build();
    }

    private static long value(Expression expression) {
        if (!expression.hasLiteral() || !expression.getLiteral().hasI64()) {
            return -1;
        }
        return Math.max(-1, expression.getLiteral().getI64());
    }
}
