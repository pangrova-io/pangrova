package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.protocol.rest.maintenance.ReclusterStageRequest;
import org.pangrova.protocol.rest.maintenance.ReclusterStageResult;
import org.pangrova.worker.catalog.WorkerCatalogRuntime;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Semaphore;

/** Internal bounded endpoint for staging connector maintenance output. */
@Path(ReclusterStageRequest.PATH)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public final class ReclusterStageResource {
    private static final int PAYLOAD_TOO_LARGE = 413;
    private static final int TOO_MANY_REQUESTS = 429;
    private final WorkerCatalogRuntime catalogs;
    private final Semaphore admission = new Semaphore(1);

    public ReclusterStageResource(WorkerCatalogRuntime catalogs) {
        this.catalogs = java.util.Objects.requireNonNull(catalogs, "catalogs");
    }

    /** Claims one staging slot and returns the exact attempt's connector file descriptors. */
    @POST
    public Response stage(InputStream input) throws IOException {
        byte[] bytes = input.readNBytes(ReclusterStageRequest.MAX_REQUEST_BYTES + 1);
        if (bytes.length > ReclusterStageRequest.MAX_REQUEST_BYTES) {
            return Response.status(PAYLOAD_TOO_LARGE).build();
        }
        if (!admission.tryAcquire()) {
            return Response.status(TOO_MANY_REQUESTS).build();
        }
        try {
            var mapper = JsonMapperFactory.shared();
            var work = mapper.readValue(bytes, ReclusterStageRequest.class);
            var staged = new ReclusterStageResult(work.attemptId(), catalogs.stageRecluster(work));
            byte[] result = mapper.writeValueAsBytes(staged);
            if (result.length > ReclusterStageRequest.MAX_RESULT_BYTES) {
                throw new IllegalStateException("RECLUSTER result exceeds its admitted size");
            }
            return Response.ok(result, MediaType.APPLICATION_JSON).build();
        } finally {
            admission.release();
        }
    }
}
