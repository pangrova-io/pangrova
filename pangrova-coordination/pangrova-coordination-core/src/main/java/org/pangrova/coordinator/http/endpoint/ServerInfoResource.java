package org.pangrova.coordinator.http.endpoint;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.protocol.rest.version.ServerInfo;

/**
 * JAX-RS resource exposing server version and API compatibility information.
 *
 * <p>This endpoint is exempt from authentication and API version checking so that
 * clients can discover server capabilities before authenticating or negotiating
 * a protocol version. Load balancers and orchestrators can also use this endpoint
 * for readiness discovery.
 *
 * <p>Response example:
 * <pre>{@code
 * {
 *   "server_version": "0.1.0-SNAPSHOT",
 *   "api_version": 1,
 *   "min_api_version": 1,
 *   "server_name": "Pangrova"
 * }
 * }</pre>
 */
@Path("/v1/server/info")
@Produces(MediaType.APPLICATION_JSON)
public final class ServerInfoResource {

    private final ServerInfo serverInfo;

    public ServerInfoResource(String serverVersion) {
        this.serverInfo = ServerInfo.current(serverVersion);
    }

    public ServerInfoResource(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
    }

    @GET
    public Response getServerInfo() {
        return Response.ok(serverInfo).build();
    }
}
