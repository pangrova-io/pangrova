package org.pangrova.client.http;

import org.pangrova.common.client.session.RetryPolicy;

/**
 * HTTP status code constants shared across the REST client.
 */
final class HttpStatusCodes {

    /** 200 OK. */
    static final int OK = 200;
    /** 202 Accepted. */
    static final int ACCEPTED = 202;
    /** 300 Multiple Choices — lower bound of the 3xx redirection class. */
    static final int MULTIPLE_CHOICES = 300;
    /** 400 Bad Request — lower bound of the 4xx client error class. */
    static final int BAD_REQUEST = 400;
    /** 404 Not Found. */
    static final int NOT_FOUND = 404;
    /** 429 Too Many Requests. */
    static final int TOO_MANY_REQUESTS = 429;
    /** 500 Internal Server Error — lower bound of the 5xx server error class. */
    static final int INTERNAL_SERVER_ERROR = 500;
    /** 502 Bad Gateway. */
    static final int BAD_GATEWAY = 502;
    /** 503 Service Unavailable. */
    static final int SERVICE_UNAVAILABLE = 503;
    /** 504 Gateway Timeout. */
    static final int GATEWAY_TIMEOUT = 504;

    private HttpStatusCodes() {}

    /**
     * Determines whether the given status code represents a transient
     * server-side condition that is safe to retry.
     *
     * @param statusCode the HTTP status code to evaluate
     * @return true if the request may be retried
     */
    static boolean isRetryable(int statusCode) {
        return RetryPolicy.isRetryableStatus(statusCode);
    }
}
