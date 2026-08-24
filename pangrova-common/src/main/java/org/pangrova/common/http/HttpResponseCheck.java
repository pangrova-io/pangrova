package org.pangrova.common.http;

/**
 * Utility for classifying HTTP response status codes.
 *
 * <p>Provides consistent checks across modules, replacing inline comparisons
 * like {@code statusCode >= 200 && statusCode < 300} or {@code statusCode == 200}.
 * Using the 2xx range (not just 200) prevents subtle bugs where {@code 201 Created}
 * or {@code 204 No Content} is incorrectly treated as failure.
 *
 * <p>This is a stateless utility class and cannot be instantiated.
 */
public final class HttpResponseCheck {

    private static final int SUCCESS_MIN = 200;
    private static final int SUCCESS_MAX_EXCLUSIVE = 300;
    private static final int CLIENT_ERROR_MIN = 400;
    private static final int SERVER_ERROR_MIN = 500;

    private HttpResponseCheck() {}

    /**
     * Returns {@code true} if the status code indicates a successful response (2xx).
     *
     * @param statusCode the HTTP response status code
     * @return {@code true} for status codes in the range [200, 299]
     */
    public static boolean isSuccess(int statusCode) {
        return statusCode >= SUCCESS_MIN && statusCode < SUCCESS_MAX_EXCLUSIVE;
    }

    /**
     * Returns {@code true} if the status code indicates a client error (4xx).
     *
     * @param statusCode the HTTP response status code
     * @return {@code true} for status codes in the range [400, 499]
     */
    public static boolean isClientError(int statusCode) {
        return statusCode >= CLIENT_ERROR_MIN && statusCode < SERVER_ERROR_MIN;
    }

    /**
     * Returns {@code true} if the status code indicates a server error (5xx).
     *
     * @param statusCode the HTTP response status code
     * @return {@code true} for status codes 500 and above
     */
    public static boolean isServerError(int statusCode) {
        return statusCode >= SERVER_ERROR_MIN;
    }
}
