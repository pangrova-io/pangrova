package org.pangrova.client.http;

import org.pangrova.common.client.constants.ApiVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/** The response advertises the server current version. HTTP 422 signals an incompatible request. */
final class ApiVersionHeaders {
    private static final Logger LOG = LoggerFactory.getLogger(ApiVersionHeaders.class);

    private ApiVersionHeaders() {}

    static void addVersionHeader(HttpRequest.Builder builder) {
        builder.setHeader(ApiVersion.HEADER_NAME, Integer.toString(ApiVersion.CURRENT));
    }

    static void checkResponseVersion(HttpResponse<?> response) {
        var values = response.headers().allValues(ApiVersion.HEADER_NAME);
        if (values.isEmpty()) return;
        if (values.size() == 1) {
            try {
                int serverVersion = Integer.parseInt(values.get(0).trim());
                if (serverVersion != ApiVersion.CURRENT) {
                    LOG.warn("event=api_version_mismatch clientVersion={} serverVersion={} host={}",
                            ApiVersion.CURRENT, serverVersion, response.uri().getHost());
                }
                return;
            } catch (NumberFormatException ignored) {
                // Multiple or malformed values cannot identify a single server version.
            }
        }
        LOG.warn("event=api_version_parse_error host={}", response.uri().getHost());
    }
}
