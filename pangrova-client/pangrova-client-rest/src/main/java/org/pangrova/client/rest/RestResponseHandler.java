package org.pangrova.client.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.protocol.rest.error.ErrorDetail;

import java.net.http.HttpResponse;

/**
 * Validates HTTP responses and maps error payloads to client exceptions.
 * Package-private helper extracted from {@link PangrovaRestClient}.
 */
final class RestResponseHandler {

    private final ObjectMapper objectMapper;

    RestResponseHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Deserializes a response body into the requested type.
     *
     * @param body         the raw response body
     * @param responseType target type
     * @param context      human-readable request description for error messages
     * @param <T>          response type
     * @return the deserialized response object
     */
    <T> T deserialize(String body, Class<T> responseType, String context) {
        try {
            return objectMapper.readValue(body, responseType);
        } catch (Exception e) {
            throw new PangrovaProtocolException(
                    "DESERIALIZATION_FAILED",
                    "Failed to deserialize response for " + context, HttpStatusCodes.OK, e);
        }
    }

    /**
     * Verifies the response status is 2xx, mapping error responses to exceptions.
     *
     * @param response the HTTP response to check
     */
    void check(HttpResponse<String> response) {
        int statusCode = response.statusCode();
        if (statusCode >= HttpStatusCodes.OK && statusCode < HttpStatusCodes.MULTIPLE_CHOICES) {
            return;
        }
        if (statusCode >= HttpStatusCodes.INTERNAL_SERVER_ERROR) {
            throw parseServerError(response);
        }
        if (statusCode >= HttpStatusCodes.BAD_REQUEST) {
            throw parseClientHttpError(response);
        }
        throw new PangrovaProtocolException("UNEXPECTED_STATUS",
                "Unexpected HTTP status " + statusCode + ": " + response.body(), statusCode);
    }

    private PangrovaServerException parseServerError(HttpResponse<String> response) {
        try {
            ErrorDetail error = objectMapper.readValue(response.body(), ErrorDetail.class);
            return new PangrovaServerException(error.errorCode(), error.message(), response.statusCode());
        } catch (Exception e) {
            return new PangrovaServerException("SERVER_ERROR",
                    "HTTP " + response.statusCode() + ": " + response.body(), response.statusCode());
        }
    }

    private PangrovaClientException parseClientHttpError(HttpResponse<String> response) {
        try {
            ErrorDetail error = objectMapper.readValue(response.body(), ErrorDetail.class);
            return new PangrovaClientException(error.errorCode(), error.message());
        } catch (Exception e) {
            return new PangrovaProtocolException("HTTP_" + response.statusCode(),
                    "HTTP " + response.statusCode() + ": " + response.body(), response.statusCode());
        }
    }
}
