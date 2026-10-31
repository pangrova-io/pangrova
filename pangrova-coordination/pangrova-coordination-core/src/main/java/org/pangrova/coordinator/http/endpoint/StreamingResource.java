package org.pangrova.coordinator.http.endpoint;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.streaming.api.cdc.CdcLifecycleManager;
import org.pangrova.streaming.api.cdc.CdcLifecycleManager.CdcRegistrationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JAX-RS resource for CDC stream registration and heartbeat.
 * Delegates to {@link CdcLifecycleManager} SPI.
 */
@Path("/v1/streaming")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class StreamingResource {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingResource.class);

    private final CdcLifecycleManager cdcManager;

    public StreamingResource(CdcLifecycleManager cdcManager) {
        this.cdcManager = checkNotNull(cdcManager, "cdcManager must not be null");
    }

    @POST
    @Path("/cdc/register")
    public Response registerCdc(JsonNode node) {
        String consumerId = node.path("consumer_id").asText(null);
        String tenantId = node.path("tenant_id").asText(null);
        int maxWrites = node.path("max_concurrent_writes").asInt(4);
        long maxBytes = node.path("max_pending_bytes").asLong(67_108_864L);

        CdcRegistrationResult result = cdcManager.register(
                consumerId, tenantId, maxWrites, maxBytes);
        LOG.info("CDC stream registered: {}", result.streamId());
        return Response.ok(result).build();
    }

    @POST
    @Path("/cdc/heartbeat")
    public Response cdcHeartbeat(JsonNode node) {
        String streamId = node.path("stream_id").asText();
        cdcManager.heartbeat(streamId);
        return Response.noContent().build();
    }
}
