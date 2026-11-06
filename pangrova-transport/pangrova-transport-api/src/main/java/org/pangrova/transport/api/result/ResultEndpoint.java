package org.pangrova.transport.api.result;

import org.pangrova.common.api.annotation.Internal;

/**
 * Endpoint where query results can be fetched via transport.
 */
@Internal
public record ResultEndpoint(
    String host,
    int port,
    String ticket,
    String protocol
) {}
