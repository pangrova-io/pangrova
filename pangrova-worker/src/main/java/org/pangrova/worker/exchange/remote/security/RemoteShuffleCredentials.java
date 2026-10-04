package org.pangrova.worker.exchange.remote.security;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Arrays;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import io.grpc.ChannelCredentials;
import io.grpc.TlsChannelCredentials;

/** Loads the configured exact trust/key material before channel admission. */
public final class RemoteShuffleCredentials {
    private RemoteShuffleCredentials() {}

    public static ChannelCredentials fromPem(Path rootCa, Path certificateChain, Path privateKey) {
        try {
            return TlsChannelCredentials.newBuilder().trustManager(rootCa.toFile())
                    .keyManager(certificateChain.toFile(), privateKey.toFile()).build();
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("Failed to load PEM shuffle TLS material", failure);
        }
    }

    public static ChannelCredentials fromStores(ShuffleClientTlsConfig config) {
        try {
            var builder = TlsChannelCredentials.newBuilder();
            var trustStore = loadStore(config.trustStorePath(), config.trustStorePassword());
            var trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(trustStore);
            builder.trustManager(trust.getTrustManagers());
            if (config.mtlsEnabled()) configureKey(builder, config);
            return builder.build();
        } catch (Exception failure) {
            throw new IllegalStateException("Failed to build TLS credentials for shuffle client. "
                    + "trustStore=" + config.trustStorePath() + " keyStore=" + config.keyStorePath(), failure);
        }
    }

    private static void configureKey(TlsChannelCredentials.Builder builder, ShuffleClientTlsConfig config)
            throws java.security.GeneralSecurityException, java.io.IOException {
        var keyStore = loadStore(config.keyStorePath(), config.keyStorePassword());
        char[] password = config.keyStorePassword() == null ? null : config.keyStorePassword().toCharArray();
        try {
            var keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keys.init(keyStore, password);
            builder.keyManager(keys.getKeyManagers());
        } finally { if (password != null) Arrays.fill(password, '\0'); }
    }

    private static KeyStore loadStore(String path, String password) {
        char[] characters = password == null ? null : password.toCharArray();
        try {
            var store = KeyStore.getInstance(path.endsWith(".p12") || path.endsWith(".pfx") ? "PKCS12" : "JKS");
            try (var stream = Files.newInputStream(Path.of(path))) { store.load(stream, characters); }
            return store;
        } catch (Exception failure) {
            throw new IllegalStateException("Failed to load key store from " + path, failure);
        } finally { if (characters != null) Arrays.fill(characters, '\0'); }
    }
}
