package org.pangrova.common.client.query;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.net.URISyntaxException;

/** The Coordinator incarnation and HTTP address that issued a query receipt. */
public record QueryOwnerEndpoint(
        @JsonProperty("coordinator_id") String coordinatorId,
        @JsonProperty("incarnation") String incarnation,
        @JsonProperty("host") String host,
        @JsonProperty("port") int port) {
    private static final int MAX_PORT = 65_535;

    public QueryOwnerEndpoint {
        if (coordinatorId == null || coordinatorId.isBlank() || incarnation == null || incarnation.isBlank()) {
            throw new IllegalArgumentException("Query owner requires a Coordinator and incarnation");
        }
        if (host == null || host.isBlank() || port <= 0 || port > MAX_PORT) {
            throw new IllegalArgumentException("Query owner requires a valid HTTP host and port");
        }
        endpointUri("http", host, port);
    }

    /** Constructs the origin over the client's configured transport scheme. */
    public URI origin(String scheme) {
        return endpointUri(scheme, host, port);
    }

    private static URI endpointUri(String scheme, String host, int port) {
        try {
            return new URI(scheme, null, host, port, null, null, null);
        } catch (URISyntaxException invalid) {
            throw new IllegalArgumentException("Invalid query owner HTTP endpoint", invalid);
        }
    }
}
