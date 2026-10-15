package org.pangrova.coordinator.http.endpoint;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.pangrova.operations.bootstrap.HealthCheckEndpoint;
import org.pangrova.operations.bootstrap.HealthStatus;

/**
 * JAX-RS resource for Coordinator health check.
 * Delegates to {@link HealthCheckEndpoint}.
 */
@Path("/v1/health")
@Produces(MediaType.APPLICATION_JSON)
public final class CoordinatorHealthResource {

    private final HealthCheckEndpoint healthCheckEndpoint;

    public CoordinatorHealthResource(HealthCheckEndpoint healthCheckEndpoint) {
        this.healthCheckEndpoint = checkNotNull(
                healthCheckEndpoint, "healthCheckEndpoint must not be null");
    }

    @GET
    public Response health() {
        HealthStatus status = healthCheckEndpoint.health();
        int httpStatus = status.ready() ? 200 : 503;
        return Response.status(httpStatus).entity(status).build();
    }

    @GET
    @Path("live")
    public Response live() {
        boolean live = healthCheckEndpoint.isLive();
        return Response.status(live ? 200 : 503).entity(Map.of("live", live)).build();
    }
}
