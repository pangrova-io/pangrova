package org.pangrova.protocol.transport;

/**
 * Transport server configuration for SPI contract.
 *
 * @param port              gRPC listen port
 * @param tlsCertPath       path to TLS certificate file (nullable — plaintext when null)
 * @param tlsKeyPath        path to TLS private key file (nullable — plaintext when null)
 * @param tlsTrustChainPath path to CA trust chain for client certificate validation (nullable — no mTLS when null)
 * @param mtlsEnabled       whether mutual TLS (client certificate) is required for connections
 * @param allocatorLimitBytes  maximum memory for the Arrow allocator (0 = use default 256MB)
 */
public record TransportServerConfig(
        int port,
        String tlsCertPath,
        String tlsKeyPath,
        String tlsTrustChainPath,
        boolean mtlsEnabled,
        long allocatorLimitBytes) {

    /** Convenience constructor without TLS parameters. */
    public TransportServerConfig(int port) {
        this(port, null, null, null, false, 0L);
    }

    /** Convenience constructor with TLS but without mTLS. */
    public TransportServerConfig(int port, String tlsCertPath, String tlsKeyPath) {
        this(port, tlsCertPath, tlsKeyPath, null, false, 0L);
    }

    /** Full constructor without allocator override (backward compatible). */
    public TransportServerConfig(int port, String tlsCertPath, String tlsKeyPath,
                                 String tlsTrustChainPath, boolean mtlsEnabled) {
        this(port, tlsCertPath, tlsKeyPath, tlsTrustChainPath, mtlsEnabled, 0L);
    }

    /** Returns true if TLS is configured (cert and key paths are both non-null). */
    public boolean tlsEnabled() {
        return tlsCertPath != null && !tlsCertPath.isBlank()
                && tlsKeyPath != null && !tlsKeyPath.isBlank();
    }

    /** Returns true if mTLS is enabled and trust chain is configured. */
    public boolean mtlsConfigured() {
        return mtlsEnabled && tlsEnabled()
                && tlsTrustChainPath != null && !tlsTrustChainPath.isBlank();
    }
}
