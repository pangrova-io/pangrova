package org.pangrova.client.rest;

/**
 * Builds a {@link javax.net.ssl.SSLContext} from the client's trust store settings.
 * Package-private helper extracted from {@link PangrovaRestClient}.
 */
final class TlsContextFactory {

    private TlsContextFactory() {}

    /**
     * Builds an SSLContext from the configured trust store.
     *
     * @param config client configuration holding the trust store path and password
     * @return an SSLContext initialized from the trust store
     * @throws IllegalStateException if the trust store cannot be loaded
     */
    static javax.net.ssl.SSLContext build(PangrovaRestClientConfig config) {
        char[] password = config.trustStorePassword() != null
                ? config.trustStorePassword().toCharArray() : null;
        try {
            java.security.KeyStore trustStore = java.security.KeyStore.getInstance(
                    config.trustStorePath().endsWith(".p12") ? "PKCS12" : "JKS");
            try (java.io.InputStream is = java.nio.file.Files.newInputStream(
                    java.nio.file.Path.of(config.trustStorePath()))) {
                trustStore.load(is, password);
            }
            javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(
                    javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
            ctx.init(null, tmf.getTrustManagers(), null);
            return ctx;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build SSLContext from " + config.trustStorePath(), e);
        } finally {
            if (password != null) {
                java.util.Arrays.fill(password, '\0');
            }
        }
    }
}
