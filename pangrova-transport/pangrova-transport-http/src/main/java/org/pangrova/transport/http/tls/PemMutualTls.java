package org.pangrova.transport.http.tls;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Loads explicit PEM identity and trust roots for internal mutual-TLS HTTP. */
public final class PemMutualTls {
    private static final Pattern PRIVATE_KEY = Pattern.compile(
            "-----BEGIN PRIVATE KEY-----\\s*([A-Za-z0-9+/=\\s]+?)\\s*-----END PRIVATE KEY-----");
    private static final char[] IN_MEMORY_PASSWORD = new char[0];

    private PemMutualTls() {}

    /** No system trust fallback: both peers must present an identity under the configured CA. */
    public static SSLContext context(Path certificateChain, Path privateKey, Path rootCa) {
        Objects.requireNonNull(certificateChain, "certificateChain");
        Objects.requireNonNull(privateKey, "privateKey");
        Objects.requireNonNull(rootCa, "rootCa");
        try {
            Certificate[] chain = certificates(certificateChain);
            Certificate[] roots = certificates(rootCa);
            PrivateKey key = privateKey(privateKey);
            KeyStore identity = KeyStore.getInstance("PKCS12");
            identity.load(null, IN_MEMORY_PASSWORD);
            identity.setKeyEntry("task-control", key, IN_MEMORY_PASSWORD, chain);
            KeyStore trust = KeyStore.getInstance("PKCS12");
            trust.load(null, IN_MEMORY_PASSWORD);
            for (int index = 0; index < roots.length; index++) {
                trust.setCertificateEntry("root-" + index, roots[index]);
            }
            KeyManagerFactory keys = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm());
            keys.init(identity, IN_MEMORY_PASSWORD);
            TrustManagerFactory authorities = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            authorities.init(trust);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys.getKeyManagers(), authorities.getTrustManagers(), null);
            return context;
        } catch (Exception failure) {
            throw new IllegalArgumentException("Invalid internal mutual-TLS PEM material", failure);
        }
    }

    private static Certificate[] certificates(Path path) throws Exception {
        requireReadable(path);
        try (InputStream input = Files.newInputStream(path)) {
            List<? extends Certificate> certificates = List.copyOf(
                    CertificateFactory.getInstance("X.509").generateCertificates(input));
            if (certificates.isEmpty() || certificates.stream().anyMatch(
                    certificate -> !(certificate instanceof X509Certificate))) {
                throw new IllegalArgumentException("TLS certificate chain is empty or invalid");
            }
            return certificates.toArray(Certificate[]::new);
        }
    }

    private static PrivateKey privateKey(Path path) throws Exception {
        requireReadable(path);
        String pem = Files.readString(path, StandardCharsets.US_ASCII);
        Matcher matcher = PRIVATE_KEY.matcher(pem);
        if (!matcher.find()) {
            throw new IllegalArgumentException("TLS key requires one unencrypted PKCS#8 private key");
        }
        String encoded = matcher.group(1);
        if (matcher.find()) {
            throw new IllegalArgumentException("TLS key requires one unencrypted PKCS#8 private key");
        }
        byte[] der = Base64.getMimeDecoder().decode(encoded);
        try {
            PKCS8EncodedKeySpec specification = new PKCS8EncodedKeySpec(der);
            for (String algorithm : List.of("RSA", "EC", "Ed25519")) {
                try {
                    return KeyFactory.getInstance(algorithm).generatePrivate(specification);
                } catch (java.security.spec.InvalidKeySpecException incompatible) {
                    // Try the next supported PKCS#8 key algorithm.
                }
            }
            throw new IllegalArgumentException("Unsupported TLS private key algorithm");
        } finally {
            java.util.Arrays.fill(der, (byte) 0);
        }
    }

    private static void requireReadable(Path path) throws IOException {
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IOException("TLS material is not a readable regular file");
        }
    }
}
