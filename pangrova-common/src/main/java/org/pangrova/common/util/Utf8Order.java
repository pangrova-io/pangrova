package org.pangrova.common.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Byte ordering used for canonical identities and PostgreSQL C collation in UTF-8 databases. */
public final class Utf8Order {
    public static int compareUnsigned(String left, String right) {
        return Arrays.compareUnsigned(left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
    }

    private Utf8Order() {}
}
