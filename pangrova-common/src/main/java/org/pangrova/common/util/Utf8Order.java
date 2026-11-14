package org.pangrova.common.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

/** Byte ordering used for canonical identities and PostgreSQL C collation in UTF-8 databases. */
public final class Utf8Order {
    public static final Comparator<String> UNSIGNED = (left, right) -> Arrays.compareUnsigned(
            left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));

    private Utf8Order() {}
}
