package org.pangrova.client.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses JDBC URLs into {@link JdbcConnectionParams}.
 * <p>
 * URL format: {@code jdbc:pangrova://<host>:<port>[/<catalog>][?key=value&key2=value2]}
 */
final class JdbcUrlParser {

    static final String URL_PREFIX = "jdbc:pangrova://";

    private JdbcUrlParser() {}

    /**
     * Returns true if the URL starts with {@code jdbc:pangrova://}.
     */
    static boolean accepts(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    /**
     * Parses a JDBC URL into connection parameters.
     *
     * @param url JDBC URL in format {@code jdbc:pangrova://<host>:<port>[/<catalog>][?k=v]}
     * @return parsed connection parameters
     * @throws IllegalArgumentException if the URL is malformed
     */
    static JdbcConnectionParams parse(String url) {
        if (!accepts(url)) {
            throw new IllegalArgumentException("Invalid Pangrova JDBC URL: " + url);
        }

        URI uri = parseUri(url);
        validateHostAndPort(uri, url);

        String catalog = parseCatalog(uri.getPath());
        Map<String, String> properties = parseQueryParams(uri.getRawQuery());
        return new JdbcConnectionParams(uri.getHost(), uri.getPort(), catalog, properties);
    }

    private static URI parseUri(String url) {
        String uriPart = url.substring("jdbc:".length());
        try {
            return new URI(uriPart);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Malformed Pangrova JDBC URL: " + url, e);
        }
    }

    private static void validateHostAndPort(URI uri, String url) {
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw new IllegalArgumentException("Missing host in JDBC URL: " + url);
        }
        if (uri.getPort() < 1 || uri.getPort() > 65535) {
            throw new IllegalArgumentException("Invalid or missing port in JDBC URL: " + url);
        }
    }

    private static String parseCatalog(String path) {
        if (path == null || path.isEmpty() || "/".equals(path)) {
            return null;
        }
        // Remove leading slash
        String catalog = path.startsWith("/") ? path.substring(1) : path;
        return catalog.isEmpty() ? null : catalog;
    }

    private static Map<String, String> parseQueryParams(String query) {
        if (query == null || query.isEmpty()) {
            return Map.of();
        }
        Map<String, String> params = new LinkedHashMap<>();
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                String key = java.net.URLDecoder.decode(pair.substring(0, eq), java.nio.charset.StandardCharsets.UTF_8);
                String value = java.net.URLDecoder.decode(pair.substring(eq + 1),
                        java.nio.charset.StandardCharsets.UTF_8);
                params.put(key, value);
            }
        }
        return Map.copyOf(params);
    }
}
