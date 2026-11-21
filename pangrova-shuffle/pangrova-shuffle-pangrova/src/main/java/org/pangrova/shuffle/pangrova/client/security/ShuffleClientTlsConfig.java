package org.pangrova.shuffle.pangrova.client.security;

import static org.pangrova.common.util.Preconditions.checkArgument;

/**
 * TLS configuration for the remote shuffle client pool.
 *
 * <p>Holds paths to trust store and key store for mutual TLS authentication
 * with remote shuffle servers. Both stores use JKS or PKCS12 format.</p>
 *
 * @param trustStorePath     path to the trust store file containing trusted CA certificates
 * @param trustStorePassword password for the trust store (nullable for PKCS12 without password)
 * @param keyStorePath       path to the key store file containing the client certificate and private key (nullable for one-way TLS)
 * @param keyStorePassword   password for the key store (nullable when keyStorePath is null)
 */
public record ShuffleClientTlsConfig(
        String trustStorePath,
        String trustStorePassword,
        String keyStorePath,
        String keyStorePassword
) {

    public ShuffleClientTlsConfig {
        checkArgument(trustStorePath != null && !trustStorePath.isBlank(),
                "trustStorePath must not be null or blank when TLS is enabled");
    }

    /** Returns true if mutual TLS (client certificate) is configured. */
    public boolean mtlsEnabled() {
        return keyStorePath != null && !keyStorePath.isBlank();
    }
}
