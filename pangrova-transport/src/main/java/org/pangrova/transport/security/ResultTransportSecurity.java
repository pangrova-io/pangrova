package org.pangrova.transport.security;

import java.nio.file.Path;

/**
 * Immutable result-channel security policy, independent of REST trust stores.
 *
 * @param requireTls reject endpoints advertising plaintext result transport
 * @param rootCaPath PEM trust roots, or null to use the system trust roots
 * @param certificateChainPath PEM client certificate chain, or null without mutual TLS
 * @param privateKeyPath PEM client private key, paired with the certificate chain
 */
public record ResultTransportSecurity(
        boolean requireTls, Path rootCaPath, Path certificateChainPath, Path privateKeyPath) {

    /** Rejects incomplete certificate pairs and prevents configured material on plaintext. */
    public ResultTransportSecurity {
        if ((certificateChainPath == null) != (privateKeyPath == null)) {
            throw new IllegalArgumentException("Result client certificate chain and private key must be supplied together");
        }
        if (certificateChainPath != null && rootCaPath == null) {
            throw new IllegalArgumentException("Result mutual TLS requires explicit trust roots");
        }
        requireNonEmptyPath(rootCaPath);
        requireNonEmptyPath(certificateChainPath);
        requireNonEmptyPath(privateKeyPath);
        requireTls = requireTls || rootCaPath != null || certificateChainPath != null;
    }

    /**
     * Creates a policy using system trust roots without client certificate material.
     *
     * @param requireTls whether plaintext result endpoints are prohibited
     * @return the result security policy
     */
    public static ResultTransportSecurity systemTrust(boolean requireTls) {
        return new ResultTransportSecurity(requireTls, null, null, null);
    }

    /**
     * Adds a caller TLS requirement without weakening an existing policy.
     *
     * @param required whether the caller additionally requires TLS
     * @return a policy retaining all certificate material and TLS requirements
     */
    public ResultTransportSecurity requiringTls(boolean required) {
        return new ResultTransportSecurity(requireTls || required, rootCaPath, certificateChainPath, privateKeyPath);
    }

    @Override
    public String toString() {
        return "ResultTransportSecurity[requireTls=" + requireTls
                + ", trustRoots=" + (rootCaPath == null ? "system" : "configured")
                + ", clientCertificate=" + (certificateChainPath == null ? "absent" : "configured")
                + ", privateKey=<redacted>]";
    }

    private static void requireNonEmptyPath(Path path) {
        if (path != null && path.toString().isBlank()) {
            throw new IllegalArgumentException("Result TLS material path must not be empty");
        }
    }
}
