package org.pangrova.shuffle.api;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/** Control-plane address of an external shuffle service. */
public record ShuffleServiceAddress(String host, int port, String tlsIdentity) {

    private static final int MAX_PORT = 65_535;

    public ShuffleServiceAddress {
        checkNotNull(host, "host must not be null");
        checkNotNull(tlsIdentity, "tlsIdentity must not be null");
        host = host.trim();
        tlsIdentity = tlsIdentity.trim();
        if (host.isEmpty() || port <= 0 || port > MAX_PORT) {
            throw new IllegalArgumentException("invalid shuffle service address");
        }
    }
}
