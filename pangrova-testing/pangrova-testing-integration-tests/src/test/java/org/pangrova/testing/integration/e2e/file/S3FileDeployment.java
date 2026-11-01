package org.pangrova.testing.integration.e2e.file;

import org.pangrova.ai.worker.blob.TenantCapabilitySigner;
import org.pangrova.testing.integration.e2e.SharedTestContainers;
import org.pangrova.worker.nativebridge.jni.NativeRuntime;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.math.BigDecimal;
import java.time.Duration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** One real HTTPS/SSE MinIO deployment retained by the shared production FILE callback owner. */
public final class S3FileDeployment implements FileStorageDeployment {
    public static final String CREDENTIAL_LOCK = "pangrova-shuffle-acceptance-aws-properties";
    private static final String ACCESS_KEY = "pangrova-governed-file-test";
    private static final String PREFIX = "sql-file-acceptance";
    private final String bucket = "governed-file-" + UUID.randomUUID();
    private GenericContainer<?> container;
    private S3Client objects;
    private HttpClient metrics;
    private Path directory;
    private SSLContext previousTls;
    private String previousTrustStore;
    private String previousTrustStoreType;
    private String previousTrustStorePassword;
    private boolean trustStoreInstalled;
    private String previousAccess;
    private String previousSecret;
    private String previousSessionToken;
    private boolean credentialsInstalled;
    private boolean bucketCreated;

    /** An absent FILE deployment leaves other existing shared test deployments unchanged. */
    @Override
    public Map<String, String> prepareConfiguredDeployment() throws Exception {
        String encoded = System.getenv(CAPABILITY_KEY_ENV);
        if (encoded == null || encoded.isBlank()) {
            return Map.of();
        }
        if (objects != null) {
            return configuration();
        }
        if (NativeRuntime.isInitialized()) {
            throw new IllegalStateException("FILE deployment must precede the shared Native startup");
        }
        byte[] capabilityKey = TenantCapabilitySigner.decodeBase64Secret(encoded);
        Arrays.fill(capabilityKey, (byte) 0);
        try {
            directory = Files.createTempDirectory("pangrova-governed-file-e2e-");
            installTrustedFixtureCertificate();
            String secret = randomBase64();
            installCredentials(secret);
            container = new GenericContainer<>("minio/minio:RELEASE.2024-12-18T13-15-44Z")
                    .withNetwork(SharedTestContainers.NETWORK).withExposedPorts(9000)
                    .withEnv("MINIO_ROOT_USER", ACCESS_KEY).withEnv("MINIO_ROOT_PASSWORD", secret)
                    .withEnv("MINIO_KMS_SECRET_KEY", "pangrova-file-e2e:" + randomBase64())
                    .withEnv("MINIO_PROMETHEUS_AUTH_TYPE", "public")
                    .withCopyFileToContainer(MountableFile.forClasspathResource(
                            "shuffle-fixture/tls/localhost-cert.pem", 0644), "/certs/public.crt")
                    .withCopyFileToContainer(MountableFile.forClasspathResource(
                            "shuffle-fixture/tls/localhost-key.pem", 0600), "/certs/private.key")
                    .withCommand("server", "--certs-dir", "/certs", "/data")
                    .waitingFor(Wait.forListeningPort());
            container.start();
            if (!List.of("localhost", "127.0.0.1", "::1").contains(container.getHost())) {
                throw new IllegalStateException("The existing localhost TLS fixture requires a local Docker host");
            }
            objects = S3Client.builder().region(Region.US_EAST_1).endpointOverride(endpoint())
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build()).build();
            objects.createBucket(request -> request.bucket(bucket));
            bucketCreated = true;
            metrics = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            return configuration();
        } catch (Exception | Error failure) {
            try {
                close();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private Map<String, String> configuration() {
        return Map.ofEntries(
                Map.entry("file.storage-backend", "s3"),
                Map.entry("file.storage-staging-root", directory.resolve("staging").toString()),
                Map.entry("file.capability-secret-reference", "env://" + CAPABILITY_KEY_ENV),
                Map.entry("file.storage.s3.bucket", bucket),
                Map.entry("file.storage.s3.region", "us-east-1"),
                Map.entry("file.storage.s3.endpoint", endpoint().toString()),
                Map.entry("file.storage.s3.prefix", PREFIX),
                Map.entry("file.storage.s3.sse", "AES256"),
                Map.entry("file.storage.s3.path-style-access", "true"));
    }

    public void requireConfiguredDeployment() {
        if (objects == null || !bucketCreated) {
            throw new IllegalStateException("Real FILE SQL E2E requires " + CAPABILITY_KEY_ENV
                    + " before shared Native startup; absence cannot skip FILE acceptance");
        }
    }

    /** Reads the real bucket, including all pages; this is the publication cardinality oracle. */
    public List<String> publishedKeys() {
        requireConfiguredDeployment();
        return objects.listObjectsV2Paginator(request -> request.bucket(bucket).prefix(PREFIX + "/"))
                .contents().stream().map(value -> value.key()).sorted().toList();
    }

    public byte[] readPublishedObject(String key) {
        requireConfiguredDeployment();
        return objects.getObjectAsBytes(request -> request.bucket(bucket).key(key)).asByteArray();
    }

    public HeadObjectResponse publishedObjectMetadata(String key) {
        requireConfiguredDeployment();
        return objects.headObject(request -> request.bucket(bucket).key(key).checksumMode("ENABLED"));
    }

    /** Actual server PUT count, including rejected conditional PUTs; metrics are cached for ten seconds. */
    public long putRequestCount() throws Exception {
        requireConfiguredDeployment();
        var response = metrics.send(HttpRequest.newBuilder(endpoint().resolve("/minio/v2/metrics/node"))
                .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Actual MinIO metrics returned HTTP " + response.statusCode());
        }
        boolean hasS3Requests = false;
        long count = 0;
        for (String line : response.body().lines().toList()) {
            if (!line.startsWith("minio_s3_requests_total{")) {
                continue;
            }
            hasS3Requests = true;
            int closing = line.indexOf('}');
            if (closing < 0) {
                throw new IllegalStateException("Malformed actual MinIO S3 request metric");
            }
            String labels = line.substring(line.indexOf('{') + 1, closing);
            if (Arrays.asList(labels.split(",")).contains("api=\"putobject\"")) {
                long sample = new BigDecimal(line.substring(closing + 1).strip()).longValueExact();
                if (sample < 0) throw new IllegalStateException("Negative actual MinIO PUT counter");
                count = Math.addExact(count, sample);
            }
        }
        if (!hasS3Requests) {
            throw new IllegalStateException("Actual MinIO metrics must expose completed S3 requests");
        }
        return count;
    }

    private URI endpoint() {
        return URI.create("https://localhost:" + container.getMappedPort(9000));
    }

    private void installCredentials(String secret) {
        previousAccess = System.getProperty("aws.accessKeyId");
        previousSecret = System.getProperty("aws.secretAccessKey");
        previousSessionToken = System.getProperty("aws.sessionToken");
        System.setProperty("aws.accessKeyId", ACCESS_KEY);
        System.setProperty("aws.secretAccessKey", secret);
        System.clearProperty("aws.sessionToken");
        credentialsInstalled = true;
    }

    private void installTrustedFixtureCertificate() throws Exception {
        previousTls = SSLContext.getDefault();
        var trust = KeyStore.getInstance(KeyStore.getDefaultType());
        trust.load(null, null);
        var defaults = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        defaults.init((KeyStore) null);
        int ordinal = 0;
        for (var manager : defaults.getTrustManagers()) {
            if (manager instanceof X509TrustManager x509) {
                for (var certificate : x509.getAcceptedIssuers()) {
                    trust.setCertificateEntry("default-" + ordinal++, certificate);
                }
            }
        }
        try (var input = getClass().getResourceAsStream("/shuffle-fixture/tls/localhost-cert.pem")) {
            if (input == null) throw new IllegalStateException("Missing actual localhost TLS fixture");
            var certificate = CertificateFactory.getInstance("X.509").generateCertificate(input);
            trust.setCertificateEntry("actual-minio-localhost", certificate);
        }
        previousTrustStore = System.getProperty("javax.net.ssl.trustStore");
        previousTrustStoreType = System.getProperty("javax.net.ssl.trustStoreType");
        previousTrustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
        String password = randomBase64();
        Path storePath = directory.resolve("trusted-minio.p12");
        char[] characters = password.toCharArray();
        try (var output = Files.newOutputStream(storePath)) {
            trust.store(output, characters);
        } finally { Arrays.fill(characters, '\0'); }
        System.setProperty("javax.net.ssl.trustStore", storePath.toString());
        System.setProperty("javax.net.ssl.trustStoreType", trust.getType());
        System.setProperty("javax.net.ssl.trustStorePassword", password);
        trustStoreInstalled = true;
        var managers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        managers.init(trust);
        var configured = SSLContext.getInstance("TLS");
        configured.init(null, managers.getTrustManagers(), new SecureRandom());
        SSLContext.setDefault(configured);
    }

    private static String randomBase64() {
        byte[] value = new byte[32];
        new SecureRandom().nextBytes(value);
        try { return Base64.getEncoder().encodeToString(value); }
        finally { Arrays.fill(value, (byte) 0); }
    }

    @Override
    public void close() throws Exception {
        if (NativeRuntime.isInitialized()) {
            throw new IllegalStateException("Native shutdown and callback drain must precede FILE storage cleanup");
        }
        Exception failure = null;
        try {
            if (objects != null && bucketCreated) {
                for (String key : publishedKeys()) {
                    objects.deleteObject(request -> request.bucket(bucket).key(key));
                }
                objects.deleteBucket(request -> request.bucket(bucket));
                bucketCreated = false;
            }
        } catch (Exception cleanup) { failure = cleanup; }
        try {
            if (objects != null) objects.close();
        } catch (Exception cleanup) {
            failure = accumulate(failure, cleanup);
        }
        objects = null;
        metrics = null;
        // Every production S3 client is already closed by the Native callback owner.
        try {
            if (previousTls != null) {
                SSLContext.setDefault(previousTls);
                previousTls = null;
            }
            if (trustStoreInstalled) {
                restore("javax.net.ssl.trustStore", previousTrustStore);
                restore("javax.net.ssl.trustStoreType", previousTrustStoreType);
                restore("javax.net.ssl.trustStorePassword", previousTrustStorePassword);
                trustStoreInstalled = false;
            }
        } catch (Exception cleanup) {
            failure = accumulate(failure, cleanup);
        }
        try {
            if (credentialsInstalled) {
                restore("aws.accessKeyId", previousAccess);
                restore("aws.secretAccessKey", previousSecret);
                restore("aws.sessionToken", previousSessionToken);
                credentialsInstalled = false;
            }
        } catch (Exception cleanup) {
            failure = accumulate(failure, cleanup);
        }
        try {
            if (container != null) container.stop();
        } catch (Exception cleanup) {
            failure = accumulate(failure, cleanup);
        }
        container = null;
        try {
            if (directory != null) {
                try (var paths = Files.walk(directory)) {
                    for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                        Files.delete(path);
                    }
                }
                directory = null;
            }
        } catch (Exception cleanup) {
            failure = accumulate(failure, cleanup);
        }
        if (failure != null) throw failure;
    }

    private static Exception accumulate(Exception previous, Exception cleanup) {
        if (previous == null) return cleanup;
        previous.addSuppressed(cleanup);
        return previous;
    }

    private static void restore(String name, String previous) {
        if (previous == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, previous);
        }
    }
}
