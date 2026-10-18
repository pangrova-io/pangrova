package org.pangrova.spi.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/** Physical S3 source origin admitted for one exact-version COPY read. */
public final class CopySourceEndpointPolicy {
    private static final int HTTPS_PORT = 443;
    private static final int HTTP_PORT = 80;

    private CopySourceEndpointPolicy() {}

    /** Requires an explicit origin so SDK environment overrides cannot redirect the source. */
    public static URI require(String configured, boolean allowLoopbackTest) {
        if (configured == null || configured.isBlank()) {
            throw new IllegalArgumentException("COPY source profile requires an explicit HTTPS endpoint");
        }
        try {
            URI endpoint = new URI(configured);
            String scheme = endpoint.getScheme();
            String host = endpoint.getHost();
            boolean secure = "https".equalsIgnoreCase(scheme);
            boolean loopback = allowLoopbackTest && isLoopbackHttp(scheme, host);
            if ((!secure && !loopback) || !isCleanOrigin(endpoint)) {
                throw invalid();
            }
            int port = endpoint.getPort();
            if ((secure && port == HTTPS_PORT) || (loopback && port == HTTP_PORT)) {
                port = -1;
            }
            return new URI(scheme.toLowerCase(Locale.ROOT), null,
                    host.toLowerCase(Locale.ROOT), port, null, null, null);
        } catch (URISyntaxException failure) {
            throw invalid();
        }
    }

    private static boolean isLoopbackHttp(String scheme, String host) {
        return "http".equalsIgnoreCase(scheme)
                && ("127.0.0.1".equals(host) || "::1".equals(host)
                    || "localhost".equalsIgnoreCase(host));
    }

    private static boolean isCleanOrigin(URI endpoint) {
        String path = endpoint.getRawPath();
        return endpoint.getHost() != null && endpoint.getRawUserInfo() == null
                && endpoint.getRawQuery() == null && endpoint.getRawFragment() == null
                && (path == null || path.isEmpty() || "/".equals(path));
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("COPY source profile requires a credential-free HTTPS origin");
    }
}
