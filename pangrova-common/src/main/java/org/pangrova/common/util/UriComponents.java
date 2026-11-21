package org.pangrova.common.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Encoding for individual URI component values. */
public final class UriComponents {
    private UriComponents() {}

    /** Escapes a path segment or query parameter value in UTF-8, including delimiters and spaces. */
    public static String encode(String component) {
        return URLEncoder.encode(Objects.requireNonNull(component, "component"), StandardCharsets.UTF_8)
                .replace("+", "%20");
    }
}
