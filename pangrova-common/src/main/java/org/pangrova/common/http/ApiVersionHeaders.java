package org.pangrova.common.http;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side utility for REST API version negotiation.
 *
 * <p>Provides the header name and current client API version as constants, plus
 * helper methods to add the version header to outgoing requests and check the
 * version returned by the server.
 *
 * <p>The version integer matches {@code org.pangrova.protocol.rest.version.ApiVersion.CURRENT}
 * but is duplicated here so that {@code pangrova-common} does not depend on
 * {@code pangrova-protocol} (which depends on common).
 *
 * <p>Version negotiation protocol:
 * <ul>
 *   <li>Client sends {@link #HEADER_NAME} with its {@link #CLIENT_VERSION} on every request.</li>
 *   <li>Server checks the header and rejects requests with unsupported versions (HTTP 422).</li>
 *   <li>Server always returns {@link #HEADER_NAME} in the response with its current version.</li>
 *   <li>Client logs a warning if the server's version differs from the client's version.</li>
 * </ul>
 */
public final class ApiVersionHeaders {

    private static final Logger LOG = LoggerFactory.getLogger(ApiVersionHeaders.class);

    private ApiVersionHeaders() {}

    /**
     * HTTP header name used by both client and server to communicate API version.
     * Must match {@code org.pangrova.protocol.rest.version.ApiVersion.HEADER_NAME}.
     */
    public static final String HEADER_NAME = "X-Pangrova-Api-Version";

    /**
     * Current client API version. Must match
     * {@code org.pangrova.protocol.rest.version.ApiVersion.CURRENT}.
     */
    public static final int CLIENT_VERSION = 1;

    /**
     * Adds the API version header to an {@link HttpRequest.Builder}.
     *
     * @param builder the request builder to augment
     * @return the same builder for chaining
     */
    public static HttpRequest.Builder addVersionHeader(HttpRequest.Builder builder) {
        return builder.header(HEADER_NAME, String.valueOf(CLIENT_VERSION));
    }

    /**
     * Checks the server's API version from the response header and logs a warning
     * if it differs from the client's version.
     *
     * @param response the HTTP response to inspect
     */
    public static void checkResponseVersion(HttpResponse<?> response) {
        Optional<String> serverVersionHeader = response.headers()
                .firstValue(HEADER_NAME);
        if (serverVersionHeader.isEmpty()) {
            return;
        }
        try {
            int serverVersion = Integer.parseInt(serverVersionHeader.get().trim());
            if (serverVersion != CLIENT_VERSION) {
                LOG.warn("event=api_version_mismatch clientVersion={} serverVersion={} uri={}",
                        CLIENT_VERSION, serverVersion, response.uri());
            }
        } catch (NumberFormatException e) {
            LOG.warn("event=api_version_parse_error serverHeader={} uri={}",
                    serverVersionHeader.get(), response.uri());
        }
    }
}
