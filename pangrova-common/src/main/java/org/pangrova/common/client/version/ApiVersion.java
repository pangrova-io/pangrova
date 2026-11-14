package org.pangrova.common.client.version;

/**
 * Defines the REST API version contract between Pangrova clients and servers.
 *
 * <p>The API version is a single integer that increments whenever a breaking change
 * is introduced to the REST protocol (new required fields, removed endpoints, changed
 * semantics). Non-breaking additions (new optional fields, new endpoints) do NOT
 * require a version bump.
 *
 * <p>Version negotiation:
 * <ul>
 *   <li>Client sends {@link #HEADER_NAME} with its supported API version on every request.</li>
 *   <li>Server reads the header and compares to its own {@link #CURRENT}.</li>
 *   <li>If the client version is outside [{@link #MIN_SUPPORTED}, {@link #CURRENT}], the server
 *       rejects the request with HTTP 422 and error code {@code API_VERSION_UNSUPPORTED}.</li>
 *   <li>If the header is absent (legacy client), the server logs a warning and allows the
 *       request through (graceful migration).</li>
 * </ul>
 *
 * <p>The server also exposes a {@code /v1/server/info} endpoint that returns the current
 * API version and server metadata, allowing clients to discover compatibility before
 * issuing queries.
 */
public final class ApiVersion {

    private ApiVersion() {}

    /**
     * Current API version. Increment when a breaking REST protocol change is introduced.
     */
    public static final int CURRENT = 1;

    /**
     * Minimum API version the server still supports. Requests from clients with
     * a version below this are rejected.
     */
    public static final int MIN_SUPPORTED = 1;

    /**
     * HTTP header name used by clients to declare their expected API version.
     */
    public static final String HEADER_NAME = "X-Pangrova-Api-Version";

    /**
     * Error code returned when the client's API version is not supported.
     */
    public static final String ERROR_CODE_UNSUPPORTED = "API_VERSION_UNSUPPORTED";
}
