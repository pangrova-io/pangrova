package org.pangrova.protocol.rest.version;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response payload for the {@code /v1/server/info} endpoint.
 * Allows clients to discover server capabilities and version compatibility
 * before issuing queries.
 *
 * @param serverVersion     human-readable server build version (e.g. "0.1.0-SNAPSHOT")
 * @param apiVersion        current REST API version supported by this server
 * @param minApiVersion     minimum client API version accepted by this server
 * @param serverName        server product name
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ServerInfo(
        @JsonProperty("server_version") String serverVersion,
        @JsonProperty("api_version") int apiVersion,
        @JsonProperty("min_api_version") int minApiVersion,
        @JsonProperty("server_name") String serverName
) {
    /**
     * Creates a ServerInfo reflecting the current protocol constants.
     *
     * @param serverVersion build version string (from Maven artifact or runtime property)
     */
    public static ServerInfo current(String serverVersion) {
        return new ServerInfo(
                serverVersion,
                ApiVersion.CURRENT,
                ApiVersion.MIN_SUPPORTED,
                "Pangrova");
    }
}
