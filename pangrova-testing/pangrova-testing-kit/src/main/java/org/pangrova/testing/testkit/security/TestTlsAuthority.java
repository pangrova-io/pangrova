package org.pangrova.testing.testkit.security;

import org.pangrova.common.concurrent.NotThreadSafe;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Actual test-only JDK certificate authority; generate identities before publishing them to process owners. */
@NotThreadSafe
public final class TestTlsAuthority implements AutoCloseable {
    private final Path directory;
    private final Path store;
    private final Path root;
    private final char[] password = UUID.randomUUID().toString().toCharArray();

    public TestTlsAuthority(Path workspace) throws Exception {
        directory = Files.createTempDirectory(workspace, "tls-authority-",
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        store = directory.resolve("identities.p12");
        root = directory.resolve("root.pem");
        try {
            generateKey("root", "CN=Pangrova ephemeral test CA", List.of("-ext", "BC:critical=ca:true",
                    "-ext", "KU:critical=keyCertSign,cRLSign", "-validity", "3650"));
            run(List.of("-exportcert", "-alias", "root", "-rfc", "-file", root.toString()));
            certificate(root).checkValidity();
        } catch (Exception | Error failure) {
            try {
                close();
            } catch (IOException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    public Path root() {
        return root;
    }

    public Identity issue(String name, String hostname, boolean expired) throws Exception {
        if (!name.matches("[a-z0-9][a-z0-9-]*") || !hostname.matches("[A-Za-z0-9.-]+")) {
            throw new IllegalArgumentException("Invalid test certificate identity");
        }
        return issueWithSan(name, "DNS:" + hostname, expired);
    }

    public Identity issue(String name, InetAddress address, boolean expired) throws Exception {
        return issue(name, address, null, expired);
    }

    /** Issues one test certificate for an IP result endpoint and a DNS control-plane identity. */
    public Identity issue(String name, InetAddress address, String dnsIdentity, boolean expired) throws Exception {
        if (!name.matches("[a-z0-9][a-z0-9-]*") || address == null
                || address.isAnyLocalAddress() || address.isLoopbackAddress()
                || address.isLinkLocalAddress()
                || (dnsIdentity != null && !dnsIdentity.matches("[A-Za-z0-9.-]+"))) {
            throw new IllegalArgumentException("Invalid test certificate IP identity");
        }
        String ipSan = "IP:" + address.getHostAddress();
        return issueWithSan(name, dnsIdentity == null ? ipSan : ipSan + ",DNS:" + dnsIdentity, expired);
    }

    private Identity issueWithSan(String name, String san, boolean expired) throws Exception {
        generateKey(name, "CN=" + name, List.of("-validity", "30"));
        Path request = directory.resolve(name + ".csr");
        Path chain = directory.resolve(name + ".pem");
        Path key = directory.resolve(name + "-key.pem");
        run(List.of("-certreq", "-alias", name, "-file", request.toString()));
        var options = new ArrayList<>(List.of("-gencert", "-alias", "root", "-rfc", "-infile", request.toString(),
                "-outfile", chain.toString(), "-ext", "BC=ca:false", "-ext", "EKU=serverAuth,clientAuth",
                "-ext", "KU=digitalSignature,keyEncipherment", "-ext", "SAN=" + san));
        if (expired) {
            options.addAll(List.of("-startdate", "2020/01/01 00:00:00", "-validity", "1"));
        } else {
            options.addAll(List.of("-validity", "30"));
        }
        run(options);
        exportPrivateKey(name, key);
        X509Certificate leaf = certificate(chain);
        leaf.verify(certificate(root).getPublicKey());
        if (expired) {
            if (!leaf.getNotAfter().toInstant().isBefore(Instant.now())) {
                throw new IllegalStateException("Leaf is not expired");
            }
        } else {
            leaf.checkValidity();
        }
        return new Identity(chain, key);
    }

    private void exportPrivateKey(String name, Path key) throws Exception {
        var keystore = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(store)) {
            keystore.load(input, password);
        }
        byte[] encoded = keystore.getKey(name, password).getEncoded();
        try {
            String pem = "-----BEGIN PRIVATE KEY-----\n"
                    + Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(encoded)
                    + "\n-----END PRIVATE KEY-----\n";
            Files.createFile(key,
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            Files.writeString(key, pem, StandardCharsets.US_ASCII);
        } finally {
            Arrays.fill(encoded, (byte) 0);
        }
    }

    private void generateKey(String name, String distinguishedName, List<String> extensions) throws Exception {
        var options = new ArrayList<>(List.of("-genkeypair", "-alias", name, "-dname", distinguishedName,
                "-keyalg", "RSA", "-keysize", "2048", "-storetype", "PKCS12"));
        options.addAll(extensions);
        run(options);
    }

    private static X509Certificate certificate(Path path) throws Exception {
        try (var input = Files.newInputStream(path)) {
            return (X509Certificate) java.security.cert.CertificateFactory.getInstance("X.509")
                    .generateCertificate(input);
        }
    }

    private void run(List<String> options) throws Exception {
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.addAll(options);
        command.addAll(List.of("-keystore", store.toString(), "-storepass", new String(password), "-noprompt"));
        Path output = Files.createTempFile(directory, "keytool-", ".log");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                throw new IOException("Actual certificate command did not exit");
            }
            if (Files.exists(store)) {
                Files.setPosixFilePermissions(store, PosixFilePermissions.fromString("rw-------"));
            }
            if (process.exitValue() != 0) {
                throw new IOException("Actual certificate command failed: " + Files.readString(output));
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        } finally {
            if (process.isAlive()) {
                terminate(process);
            }
        }
    }

    private static void terminate(Process process) throws IOException {
        process.destroyForcibly();
        boolean interrupted = Thread.interrupted();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (process.isAlive()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new IOException("Certificate process is still owned");
                }
                try {
                    process.waitFor(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException restoreAfterJoin) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        if (Files.exists(directory)) {
            try (var paths = Files.list(directory)) {
                for (Path path : paths.toList()) {
                    try {
                        Files.delete(path);
                    } catch (IOException cleanupFailure) {
                        if (failure == null) {
                            failure = cleanupFailure;
                        } else {
                            failure.addSuppressed(cleanupFailure);
                        }
                    }
                }
            }
            if (failure == null) {
                Files.delete(directory);
            }
        }
        Arrays.fill(password, '\0');
        if (failure != null) {
            throw failure;
        }
    }

    public record Identity(Path certificateChain, Path privateKey) { }
}
