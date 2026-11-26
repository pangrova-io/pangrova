package org.pangrova.common.client.result;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.client.enums.ResultProtocol;

/**
 * Endpoint where query results can be fetched via transport.
 */
@Internal
public record ResultEndpoint(
    String host,
    int port,
    String ticket,
    String protocol
) {
    public ResultEndpoint {
        protocol = ResultProtocol.fromValue(protocol).value();
    }
}
