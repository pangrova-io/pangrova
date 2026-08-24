package org.pangrova.protocol.rest.query;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Endpoint where query results can be fetched via transport.
 */
public record ResultEndpoint(
    @JsonProperty("host") String host,
    @JsonProperty("port") int port,
    @JsonProperty("ticket") String ticket,
    @JsonProperty("protocol") String protocol
) {}
