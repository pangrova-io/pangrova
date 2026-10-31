package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.worker.dataset.ControlAdmissionRejectedException;
import org.pangrova.worker.dataset.ControlDeadlineExceededException;
import org.pangrova.worker.dataset.NativeDatasetControlService;
import org.pangrova.worker.task.ControlEpochFenceException;
import org.pangrova.worker.task.ExecutionOwnerFenceException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/** One authenticated canonical binary endpoint for process-local Native metadata operations. */
@Path("/v1/native-dataset/control")
@Consumes(MediaType.APPLICATION_OCTET_STREAM)
@Produces(MediaType.APPLICATION_OCTET_STREAM)
public final class NativeDatasetControlResource {
    private static final int TOO_EARLY = 425;
    private static final int TOO_MANY_REQUESTS = 429;
    private final NativeDatasetControlService control;

    /** Creates the endpoint with the Worker-owned bounded service. */
    public NativeDatasetControlResource(NativeDatasetControlService control) {
        this.control = Objects.requireNonNull(control, "control");
    }

    /** Executes a schema fingerprint or a fenced publication command; never consumes row data. */
    @POST
    public Response execute(InputStream body) {
        try {
            return Response.ok(control.execute(body), MediaType.APPLICATION_OCTET_STREAM_TYPE).build();
        } catch (ControlAdmissionRejectedException full) {
            return rejected(TOO_MANY_REQUESTS, ErrorClass.ERROR_CLASS_ADMISSION, "METADATA_CONTROL_FULL", full, true);
        } catch (ControlDeadlineExceededException expired) {
            return rejected(Response.Status.REQUEST_TIMEOUT.getStatusCode(), ErrorClass.ERROR_CLASS_DEADLINE,
                    "METADATA_CONTROL_DEADLINE", expired, false);
        } catch (ControlEpochFenceException epoch) {
            return epochRejected(epoch);
        } catch (ExecutionOwnerFenceException owner) {
            return rejected(Response.Status.CONFLICT.getStatusCode(), ErrorClass.ERROR_CLASS_ADMISSION,
                    "STALE_EXECUTION_OWNER", owner, false);
        } catch (SecurityException denied) {
            return rejected(Response.Status.FORBIDDEN.getStatusCode(), ErrorClass.ERROR_CLASS_POLICY_DENIED,
                    "CATALOG_TENANT_DENIED", denied, false);
        } catch (IllegalArgumentException | IOException invalid) {
            return rejected(Response.Status.BAD_REQUEST.getStatusCode(), ErrorClass.ERROR_CLASS_INVALID_REQUEST,
                    "INVALID_METADATA_CONTROL", invalid, false);
        } catch (RuntimeException failure) {
            return rejected(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), ErrorClass.ERROR_CLASS_INTERNAL,
                    "METADATA_CONTROL_FAILED", failure, false);
        }
    }

    private static Response epochRejected(ControlEpochFenceException epoch) {
        int status = switch (epoch.reason()) {
            case CONTROL_PLANE_NOT_READY -> Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
            case EPOCH_AHEAD -> TOO_EARLY;
            default -> Response.Status.CONFLICT.getStatusCode();
        };
        return rejected(status, ErrorClass.ERROR_CLASS_ADMISSION, epoch.reason().name(), epoch,
                status != Response.Status.CONFLICT.getStatusCode());
    }

    private static Response rejected(int status, ErrorClass category, String code, Exception error, boolean retryable) {
        var result = RuntimeCommandResponse.newBuilder().setSuccess(false).setError(ExecutionError.newBuilder()
                .setErrorClass(category).setCode(code).setMessage(Objects.toString(error.getMessage(), code))
                .setFailedComponent("worker-native-dataset-control").setRetryable(retryable)).build();
        return Response.status(status).type(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .entity(result.toByteArray()).build();
    }
}
