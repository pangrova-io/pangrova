package org.pangrova.coordinator.http.endpoint;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.task.UnregisteredWorkerException;
import org.pangrova.coordinator.scheduler.failure.NoAvailableWorkerException;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.AuthenticationRequiredException;
import org.pangrova.security.tenant.quota.TenantQuotaExceededException;
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
 *   <li>All other exceptions → 500 INTERNAL_ERROR</li>
 * </ul>
 *
 * <p>Logs 5xx errors at ERROR level, 4xx at WARN level.</p>
 */
@Provider
public final class CoordinatorExceptionMapper implements ExceptionMapper<Exception> {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorExceptionMapper.class);

    private static final int HTTP_STATUS_BAD_REQUEST = 400;
    private static final int HTTP_STATUS_UNAUTHORIZED = 401;
    private static final int HTTP_STATUS_FORBIDDEN = 403;
    private static final int HTTP_STATUS_NOT_FOUND = 404;
    private static final int HTTP_STATUS_CONFLICT = 409;
    private static final int HTTP_STATUS_PAYLOAD_TOO_LARGE = 413;
    private static final int HTTP_STATUS_TOO_MANY_REQUESTS = 429;
    private static final int HTTP_STATUS_SERVICE_UNAVAILABLE = 503;
    private static final int HTTP_STATUS_INTERNAL_ERROR = 500;
    private static final int HTTP_STATUS_MAX_ERROR = 599;

    @Override
    public Response toResponse(Exception exception) {
        int status = resolveHttpStatus(exception);
        String errorCode = resolveErrorCode(exception, status);
        String errorType = resolveErrorType(status);
        String message = resolveMessage(exception);

        logException(status, errorCode, message, exception);

        ErrorDetail detail = new ErrorDetail(errorCode, errorType, message);
        Response.ResponseBuilder builder = exception instanceof jakarta.ws.rs.WebApplicationException web
                && web.getResponse().getStatus() == status
                ? Response.fromResponse(web.getResponse()) : Response.status(status);
        return builder
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
        if (exception instanceof jakarta.ws.rs.WebApplicationException web) {
            int status = web.getResponse().getStatus();
            return status >= HTTP_STATUS_BAD_REQUEST && status <= HTTP_STATUS_MAX_ERROR
                    ? status : HTTP_STATUS_INTERNAL_ERROR;
        }
        if (exception instanceof TenantQuotaExceededException) {
            return HTTP_STATUS_TOO_MANY_REQUESTS;
        }
        if (exception instanceof PangrovaException domainFailure
                && domainFailure.getErrorCode() == PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE) {
            return HTTP_STATUS_BAD_REQUEST;
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
            case HTTP_STATUS_UNAUTHORIZED -> "UNAUTHENTICATED";
            case HTTP_STATUS_FORBIDDEN -> "ACCESS_DENIED";
            case HTTP_STATUS_NOT_FOUND -> "NOT_FOUND";
            case HTTP_STATUS_CONFLICT -> "CONFLICT";
            case HTTP_STATUS_PAYLOAD_TOO_LARGE -> "PAYLOAD_TOO_LARGE";
            case HTTP_STATUS_TOO_MANY_REQUESTS -> "TOO_MANY_REQUESTS";
            case HTTP_STATUS_SERVICE_UNAVAILABLE -> "UNAVAILABLE";
            default -> "INTERNAL_ERROR";
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
