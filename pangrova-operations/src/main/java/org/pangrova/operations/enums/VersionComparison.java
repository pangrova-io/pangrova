package org.pangrova.operations.enums;

import org.pangrova.common.api.annotation.Internal;

/** Comparison applied to a semantic version range bound. */
@Internal
public enum VersionComparison {
    GTE(">="),
    LTE("<="),
    GT(">"),
    LT("<"),
    EQ("=");

    private final String symbol;

    VersionComparison(String symbol) {
        this.symbol = symbol;
    }

    public static VersionComparison fromSymbol(String symbol) {
        for (VersionComparison op : values()) {
            if (op.symbol.equals(symbol)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown operator: " + symbol);
    }
}
