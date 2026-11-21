package org.pangrova.server.coordinator.http.endpoint;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.exception.UnregisteredWorkerException;
import org.pangrova.scheduler.api.exception.NoAvailableWorkerException;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.server.coordinator.exception.RecordedSubmissionException;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.exception.AuthenticationRequiredException;
import org.pangrova.security.exception.TenantQuotaExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unified JAX-RS exception-to-JSON mapper for all Coordinator REST endpoints.
 *
 * <p>Maps exception types to HTTP status codes and produces {@link ErrorDetail} JSON responses:
 * <ul>
 *   <li>{@link IllegalArgumentException} → 400 BAD_REQUEST</li>
 *   <li>{@link AccessDeniedException} → 403 ACCESS_DENIED</li>
 *   <li>{@link TenantQuotaExceededException} → 429 USER_ERROR</li>
 *   <li>Not-found exceptions (catalog/table) → 404 NOT_FOUND</li>
 *   <li>JAX-RS exceptions retain their response status and protocol headers</li>
 *   <li>All other exceptions → 500 INTERNAL_ERROR</li>
 * </ul>
 *
 * <p>Logs 5xx errors at ERROR level, 4xx at WARN level.</p>
 */
@Provider
public final class CoordinatorExceptionMapper implements ExceptionMapper<Exception> {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorExceptionMapper.class);
    private static final String COPY_SOURCE_READ_PATH = "v1/internal/copy-source-read/";

    @Context private UriInfo requestUri;

    private static final int HTTP_STATUS_BAD_REQUEST = 400;
    private static final int HTTP_STATUS_UNAUTHORIZED = 401;
    private static final int HTTP_STATUS_FORBIDDEN = 403;
    private static final int HTTP_STATUS_NOT_FOUND = 404;
    private static final int HTTP_STATUS_METHOD_NOT_ALLOWED = 405;
    private static final int HTTP_STATUS_NOT_ACCEPTABLE = 406;
    private static final int HTTP_STATUS_PAYLOAD_TOO_LARGE = 413;
    private static final int HTTP_STATUS_CONFLICT = 409;
    private static final int HTTP_STATUS_UNSUPPORTED_MEDIA_TYPE = 415;
    private static final int HTTP_STATUS_TOO_MANY_REQUESTS = 429;
    private static final int HTTP_STATUS_SERVICE_UNAVAILABLE = 503;
    private static final int HTTP_STATUS_INTERNAL_ERROR = 500;

    @Override
    public Response toResponse(Exception exception) {
        if (exception instanceof RecordedSubmissionException recorded) {
            try (Response failure = toResponse(recorded.failure())) {
                return Response.fromResponse(failure)
                        .header("X-Pangrova-Query-Id", recorded.queryId()).build();
            }
        }
        int status = resolveHttpStatus(exception);
        String errorCode = resolveErrorCode(exception, status);
        String errorType = resolveErrorType(status);
        // JSON decoding fails before the resource method runs. Jackson can include
        // offending source text in an exception, which may contain the bearer nonce.
        boolean privateCopyRequest = requestUri != null
                && requestUri.getPath().startsWith(COPY_SOURCE_READ_PATH);
        String message = privateCopyRequest
                ? "COPY source-read request rejected" : resolveMessage(exception);

        if (privateCopyRequest) {
            LOG.warn("COPY source-read request rejected: status={} errorCode={}", status, errorCode);
        } else {
            logException(status, errorCode, message, exception);
        }

        ErrorDetail detail = new ErrorDetail(errorCode, errorType, message);
        Response.ResponseBuilder response = exception instanceof WebApplicationException webException
                ? Response.fromResponse(webException.getResponse()) : Response.status(status);
        // The JSON error replaces the original entity; retain protocol headers such as Allow,
        // WWW-Authenticate and Retry-After, but never stale body length or encoding metadata.
        return response.header("Content-Length", null).header("Content-Encoding", null)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(detail)
                .build();
    }

    private static final java.util.Map<Class<? extends Exception>, Integer> HTTP_STATUS_MAP =
            java.util.Map.of(
                    IllegalArgumentException.class, HTTP_STATUS_BAD_REQUEST,
                    AuthenticationRequiredException.class, HTTP_STATUS_UNAUTHORIZED,
                    AccessDeniedException.class, HTTP_STATUS_FORBIDDEN,
                    jakarta.ws.rs.ForbiddenException.class, HTTP_STATUS_FORBIDDEN,
                    UnregisteredWorkerException.class, HTTP_STATUS_CONFLICT,
                    NoAvailableWorkerException.class, HTTP_STATUS_SERVICE_UNAVAILABLE
            );

    private static int resolveHttpStatus(Exception exception) {
        if (exception instanceof WebApplicationException webException) {
            return webException.getResponse().getStatus();
        }
        if (exception instanceof PangrovaException failure) {
            Integer domainStatus = switch (failure.getErrorCode()) {
                case METADATA_FILE_SPACE_NOT_FOUND, FILE_OBJECT_NOT_FOUND -> HTTP_STATUS_NOT_FOUND;
                case FILE_PATH_INVALID, PARSER_SYNTAX_ERROR, PARSER_UNSUPPORTED_FEATURE -> HTTP_STATUS_BAD_REQUEST;
                case FILE_STORAGE_UNAVAILABLE, FILE_STORAGE_NOT_CONFIGURED -> HTTP_STATUS_SERVICE_UNAVAILABLE;
                default -> null;
            };
            if (domainStatus != null) return domainStatus;
        }
        if (exception instanceof TenantQuotaExceededException) {
            return HTTP_STATUS_TOO_MANY_REQUESTS;
        }
        if (exception instanceof AuthenticationRequiredException) {
            return HTTP_STATUS_UNAUTHORIZED;
        }
        if (exception instanceof AccessDeniedException) {
            return HTTP_STATUS_FORBIDDEN;
        }
        Integer status = HTTP_STATUS_MAP.get(exception.getClass());
        if (status != null) {
            return status;
        }
        if (isNotFoundException(exception)) {
            return HTTP_STATUS_NOT_FOUND;
        }
        return HTTP_STATUS_INTERNAL_ERROR;
    }

    private static String resolveErrorCode(Exception exception, int status) {
        if (exception instanceof PangrovaException pangrovaEx) {
            return pangrovaEx.getErrorCode().getCode();
        }
        return switch (status) {
            case HTTP_STATUS_BAD_REQUEST -> "BAD_REQUEST";
            case HTTP_STATUS_UNAUTHORIZED -> "AUTHENTICATION_REQUIRED";
            case HTTP_STATUS_FORBIDDEN -> "ACCESS_DENIED";
            case HTTP_STATUS_METHOD_NOT_ALLOWED -> "METHOD_NOT_ALLOWED";
            case HTTP_STATUS_NOT_ACCEPTABLE -> "NOT_ACCEPTABLE";
            case HTTP_STATUS_PAYLOAD_TOO_LARGE -> "PAYLOAD_TOO_LARGE";
            case HTTP_STATUS_UNSUPPORTED_MEDIA_TYPE -> "UNSUPPORTED_MEDIA_TYPE";
            case HTTP_STATUS_TOO_MANY_REQUESTS -> "RATE_LIMITED";
            case HTTP_STATUS_SERVICE_UNAVAILABLE -> "SERVICE_UNAVAILABLE";
            case HTTP_STATUS_NOT_FOUND -> "NOT_FOUND";
            case HTTP_STATUS_CONFLICT -> "CONFLICT";
            default -> status >= HTTP_STATUS_BAD_REQUEST && status < HTTP_STATUS_INTERNAL_ERROR
                    ? "BAD_REQUEST" : "INTERNAL_ERROR";
        };
    }

    private static String resolveErrorType(int status) {
        if (status >= HTTP_STATUS_BAD_REQUEST && status < HTTP_STATUS_INTERNAL_ERROR) {
            return "USER_ERROR";
        }
        return "INTERNAL_ERROR";
    }

    private static String resolveMessage(Exception exception) {
        String msg = exception.getMessage();
        return (msg != null) ? msg : exception.getClass().getSimpleName();
    }

    private static boolean isNotFoundException(Exception exception) {
        String className = exception.getClass().getSimpleName();
        return className.contains("NotFoundException");
    }

    private static void logException(int status, String errorCode,
                                     String message, Exception exception) {
        if (status >= HTTP_STATUS_INTERNAL_ERROR) {
            LOG.error("Internal error: errorCode={} message={}", errorCode, message, exception);
        } else {
            LOG.warn("Client error: status={} errorCode={} message={}", status, errorCode, message);
        }
    }
}
