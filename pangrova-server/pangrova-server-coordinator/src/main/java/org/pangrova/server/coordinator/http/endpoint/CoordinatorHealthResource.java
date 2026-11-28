package org.pangrova.server.coordinator.http.endpoint;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.pangrova.common.lifecycle.HealthCheckEndpoint;
import org.pangrova.common.lifecycle.HealthStatus;

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
        Response.Status httpStatus = status.ready() ? Response.Status.OK : Response.Status.SERVICE_UNAVAILABLE;
        return Response.status(httpStatus).entity(status).build();
    }

    @GET
    @Path("live")
    public Response live() {
        boolean live = healthCheckEndpoint.isLive();
        Response.Status httpStatus = live ? Response.Status.OK : Response.Status.SERVICE_UNAVAILABLE;
        return Response.status(httpStatus).entity(Map.of("live", live)).build();
    }
}
