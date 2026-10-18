package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.spi.security.AccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unified JAX-RS exception-to-JSON mapper for all Worker REST endpoints.
 *
 * <p>Maps exception types to HTTP status codes and produces {@link ErrorDetail} JSON responses:
 * <ul>
 *   <li>{@link IllegalArgumentException} → 400 BAD_REQUEST</li>
 *   <li>{@link AccessDeniedException} → 403 ACCESS_DENIED</li>
 *   <li>Not-found exceptions → 404 NOT_FOUND</li>
 *   <li>All other exceptions → 500 INTERNAL_ERROR</li>
 * </ul>
 *
 * <p>Logs 5xx errors at ERROR level, 4xx at WARN level.</p>
 */
@Provider
public final class WorkerExceptionMapper implements ExceptionMapper<Exception> {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerExceptionMapper.class);

    @Context private UriInfo requestUri;

    @Override
    public Response toResponse(Exception exception) {
        int status = resolveHttpStatus(exception);
        String errorCode = resolveErrorCode(exception, status);
        String errorType = resolveErrorType(status);
        // A malformed private task envelope can be rejected during JSON decoding,
        // before TaskResource sees it. Do not log parser source text or echo it.
        boolean privateTaskRequest = requestUri != null
                && requestUri.getPath().matches("/?v1/task(/copy-stage)?/?");
        String message = privateTaskRequest
                ? "Task submission request rejected" : resolveMessage(exception);

        if (privateTaskRequest) {
            LOG.warn("Task submission request rejected: status={} errorCode={}", status, errorCode);
        } else {
            logException(status, errorCode, message, exception);
        }

        ErrorDetail detail = new ErrorDetail(errorCode, errorType, message);
        Response.ResponseBuilder response = exception instanceof WebApplicationException webException
                ? Response.fromResponse(webException.getResponse()) : Response.status(status);
        return response.header("Content-Length", null).header("Content-Encoding", null)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(detail)
                .build();
    }

    private static int resolveHttpStatus(Exception exception) {
        if (exception instanceof WebApplicationException webException) {
            return webException.getResponse().getStatus();
        }
        if (exception instanceof org.pangrova.worker.task.ControlEpochFenceException fence) {
            return switch (fence.reason()) {
                case CONTROL_PLANE_NOT_READY -> 503;
                case EPOCH_AHEAD -> 425;
                default -> 409;
            };
        }
        if (exception instanceof org.pangrova.worker.task.ExecutionOwnerFenceException) {
            return 409;
        }
        if (exception instanceof org.pangrova.worker.task.TaskAdmissionException admission) {
            return switch (admission.reason()) {
                case EXPIRED -> 408;
                case IDENTITY_CONFLICT, SEALED -> 409;
                case CAPACITY, CLOSED -> 503;
            };
        }
        if (exception instanceof IllegalArgumentException) {
            return 400;
        }
        if (exception instanceof AccessDeniedException) {
            return 403;
        }
        if (isNotFoundException(exception)) {
            return 404;
        }
        return 500;
    }

    private static String resolveErrorCode(Exception exception, int status) {
        if (exception instanceof PangrovaException pangrovaEx) {
            return pangrovaEx.getErrorCode().getCode();
        }
        return switch (status) {
            case 400 -> "BAD_REQUEST";
            case 403 -> "ACCESS_DENIED";
            case 404 -> "NOT_FOUND";
            case 405 -> "METHOD_NOT_ALLOWED";
            case 408 -> "ADMISSION_EXPIRED";
            case 409 -> "ADMISSION_CONFLICT";
            case 425 -> "CONTROL_EPOCH_AHEAD";
            case 503 -> "SERVICE_UNAVAILABLE";
            default -> "INTERNAL_ERROR";
        };
    }

    private static String resolveErrorType(int status) {
        if (status >= 400 && status < 500) {
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
        if (status >= 500) {
            LOG.error("event=internal_error errorCode={} message={}", errorCode, message, exception);
        } else {
            LOG.warn("event=client_error status={} errorCode={} message={}", status, errorCode, message);
        }
    }
}
