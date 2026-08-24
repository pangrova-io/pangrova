package org.pangrova.protocol.rest.query;

/**
 * Canonical protocol values for result transport endpoints.
 * Eliminates scattered string literals across coordinator, worker, and client.
 */
public enum ResultProtocol {
    GRPC("grpc"),
    GRPC_TLS("grpc+tls");

    private final String value;

    ResultProtocol(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    /**
     * Returns true if this protocol requires TLS.
     */
    public boolean requiresTls() {
        return this == GRPC_TLS;
    }

    /**
     * Parse a protocol string to the corresponding enum value.
     *
     * @throws IllegalArgumentException if the value is not recognized
     */
    public static ResultProtocol fromValue(String value) {
        for (ResultProtocol p : values()) {
            if (p.value.equals(value)) {
                return p;
            }
        }
        throw new IllegalArgumentException("Unknown result protocol: " + value);
    }
}
