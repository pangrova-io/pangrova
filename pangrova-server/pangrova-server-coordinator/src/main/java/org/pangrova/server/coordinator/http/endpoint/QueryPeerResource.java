package org.pangrova.server.coordinator.http.endpoint;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.client.query.QueryOwnerEndpoint;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.function.Supplier;

/** Authenticates a signed one-hop command and delivers it only to the local query authority. */
@Path("/" + QueryPeerRouting.PATH)
@Consumes(MediaType.APPLICATION_JSON)
public final class QueryPeerResource {

    private static final String REPLY = QueryPeerResource.class.getName() + ".authenticatedReply";
    private static final Annotation[] NO_ANNOTATIONS = new Annotation[0];

    private final QueryResource queries;
    private final QueryPeerRouting peers;
    private final Supplier<QueryOwnerEndpoint> localEndpoint;

    /** Binds the relay endpoint to this server's query lifecycle and incarnation. */
    public QueryPeerResource(QueryResource queries, QueryPeerRouting peers,
            Supplier<QueryOwnerEndpoint> localEndpoint) {
        this.queries = queries;
        this.peers = peers;
        this.localEndpoint = localEndpoint;
    }

    /** Verifies the command before reading or mutating the local query. */
    @POST
    public void execute(byte[] body, @HeaderParam(QueryPeerRouting.SIGNATURE) String signature,
            @Context ContainerRequestContext request, @Suspended AsyncResponse response) throws IOException {
        var command = peers.authenticate(body, signature, localEndpoint.get());
        if (command == null) {
            response.resume(Response.status(Response.Status.FORBIDDEN).build());
            return;
        }
        request.setProperty(REPLY, new Reply(signature, command.operation()));
        String id = command.queryId();
        String principal = command.principal();
        switch (command.operation()) {
            case STATE -> queries.observeLocal(id, principal, command.observation(), response);
            case CANCEL -> response.resume(queries.cancelLocal(id, principal));
            case PROFILE -> response.resume(queries.jsonProfileLocal(id, principal));
            case FLAME_GRAPH -> response.resume(queries.flameGraphLocal(id, principal));
            default -> throw new IllegalArgumentException("Unsupported query peer operation");
        }
    }

    /** Signs the actual bytes after local observation and before the HTTP response writer. */
    public void signResponse(ContainerRequestContext request, ContainerResponseContext response) {
        if (!(request.getProperty(REPLY) instanceof Reply reply)) return;
        var entity = response.getEntity();
        byte[] body = entity instanceof byte[] bytes ? bytes
                : entity == null ? new byte[0] : QueryPeerRouting.encode(entity);
        if (body.length > QueryPeerRouting.MAX_RESPONSE_BYTES) {
            response.setStatus(Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
            body = QueryPeerRouting.encode(new ErrorDetail("QUERY_PEER_RESPONSE_LIMIT", null,
                    "Query peer response exceeds its byte limit"));
        }
        int status = response.getStatus();
        response.setEntity(body, NO_ANNOTATIONS,
                MediaType.valueOf(QueryPeerRouting.mediaType(reply.operation(), status)));
        response.getHeaders().putSingle(QueryPeerRouting.SIGNATURE,
                peers.responseSignature(reply.requestSignature(), status, body));
    }

    private record Reply(String requestSignature, QueryPeerRouting.Operation operation) {
    }
}
