package org.pangrova.testing.integration.e2e.speculation;

import org.pangrova.shuffle.config.ShuffleServiceConfig;
import org.pangrova.shuffle.server.ShuffleServerRuntime;
import org.pangrova.testing.testkit.MinioContainer;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.yaml.snakeyaml.Yaml;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/** Production shuffle service on the Coordinator's actual execution-anchor schema. */
final class ProcessRemoteShuffle implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessRemoteShuffle.class);
    static final String COORDINATOR_IDENTITY = "pangrova-coordinator.test";
    private static final String HOST = "localhost";
    private static final String ACCESS_PROPERTY = "aws.accessKeyId";
    private static final String SECRET_PROPERTY = "aws.secretAccessKey";

    private final AutoCloseable runtime;
    private final GenericContainer<?> minio;
    private final S3Client objects;
    private final String bucket;
    private final int port;
    private final String previousAccess;
    private final String previousSecret;

    private ProcessRemoteShuffle(AutoCloseable runtime, GenericContainer<?> minio,
            S3Client objects, String bucket, int port, String previousAccess, String previousSecret) {
        this.runtime = runtime;
        this.minio = minio;
        this.objects = objects;
        this.bucket = bucket;
        this.port = port;
        this.previousAccess = previousAccess;
        this.previousSecret = previousSecret;
    }

    static ProcessRemoteShuffle start(Path directory, ProcessSecurityAuthority authority,
            TestTlsAuthority tls, int port, Path java, LocalRuntimeArtifacts artifacts) throws Exception {
        String access = "shuffle-" + UUID.randomUUID().toString().substring(0, 12);
        String secret = UUID.randomUUID().toString();
        String previousAccess = System.getProperty(ACCESS_PROPERTY);
        String previousSecret = System.getProperty(SECRET_PROPERTY);
        var minio = MinioContainer.create().withExposedPorts(9000)
                .withEnv("MINIO_ROOT_USER", access).withEnv("MINIO_ROOT_PASSWORD", secret)
                .withCommand("server", "/data").waitingFor(Wait.forHttp("/minio/health/ready").forPort(9000));
        S3Client objects = null;
        try {
            minio.start();
            var endpoint = URI.create("http://" + minio.getHost() + ":" + minio.getMappedPort(9000));
            String bucket = "remote-shuffle-" + UUID.randomUUID();
            objects = S3Client.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(access, secret)))
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                    .build();
            objects.createBucket(request -> request.bucket(bucket));
            var config = config(directory, authority, tls, port, endpoint, bucket);
            AutoCloseable runtime;
            if (artifacts.shuffleExecutable().isPresent()) {
                runtime = startProcess(directory, config, java, artifacts, access, secret);
            } else {
                System.setProperty(ACCESS_PROPERTY, access);
                System.setProperty(SECRET_PROPERTY, secret);
                runtime = ShuffleServerRuntime.start(config);
            }
            return new ProcessRemoteShuffle(runtime, minio, objects, bucket, port, previousAccess, previousSecret);
        } catch (Exception | Error failure) {
            try (var failedMinio = minio; var failedObjects = objects) {
                // Close the partially initialized fixture in dependency order.
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            } finally {
                restoreCredentials(previousAccess, previousSecret);
            }
            throw failure;
        }
    }

    private static AutoCloseable startProcess(Path directory, ShuffleServiceConfig config, Path java,
            LocalRuntimeArtifacts artifacts, String access, String secret) throws Exception {
        Path yaml = writeConfiguration(directory, config);
        Path log = directory.resolve("shuffle-service.log");
        var builder = new ProcessBuilder(java.toString(), "-Xms128m", "-Xmx384m",
                "-jar", artifacts.shuffleExecutable().orElseThrow().toString(), yaml.toString());
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().put("AWS_ACCESS_KEY_ID", access);
        builder.environment().put("AWS_SECRET_ACCESS_KEY", secret);
        Process child = builder.start();
        LOG.info("event=local_process_spawn role=shuffle-service pid={} manifest={} manifestSha256={} log={}",
                child.pid(), artifacts.manifestPath(), artifacts.manifestSha256(), log);
        try {
            await("separate Shuffle process accepts connections").atMost(Duration.ofSeconds(30)).until(() -> {
                if (!child.isAlive()) throw new IllegalStateException("Shuffle process exited; log=" + log);
                try (Socket probe = new Socket()) {
                    probe.connect(new InetSocketAddress(HOST, config.server().port()), 500);
                    return true;
                } catch (IOException pending) {
                    return false;
                }
            });
            return () -> stopProcess(child);
        } catch (Exception | Error failure) {
            try {
                stopProcess(child);
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static Path writeConfiguration(Path directory, ShuffleServiceConfig config) throws IOException {
        var server = config.server();
        var database = config.database();
        var storage = config.objectStorage();
        var security = config.security();
        var limits = config.limits();
        var settings = Map.of(
                "server", Map.of("port", server.port(), "advertised-host", server.advertisedHost(),
                        "tls-identity", server.tlsIdentity()),
                "database", Map.of("jdbc-url", database.jdbcUrl(), "username", database.username(),
                        "password-file", database.passwordFile().toString(),
                        "maximum-pool-size", database.maximumPoolSize()),
                "object-storage", Map.of("bucket", storage.bucket(), "prefix", storage.prefix(),
                        "region", storage.region(), "endpoint", storage.endpoint().toString(),
                        "path-style-access", storage.pathStyleAccess(), "replication-enabled", storage.replicationEnabled(),
                        "api-call-timeout-seconds", Math.toIntExact(storage.apiCallTimeout().toSeconds()),
                        "api-call-attempt-timeout-seconds", Math.toIntExact(storage.apiCallAttemptTimeout().toSeconds())),
                "security", Map.of("certificate-chain-path", security.certificateChain().toString(),
                        "private-key-path", security.privateKey().toString(),
                        "root-ca-path", security.rootCa().toString(),
                        "capability-secret-path", security.capabilitySecret().toString(),
                        "coordinator-tls-identities", security.coordinatorTlsIdentities(),
                        "capability-ttl-seconds", Math.toIntExact(security.capabilityTtl().toSeconds())),
                "limits", Map.of("max-chunk-bytes", limits.maxChunkBytes(), "max-batch-bytes", limits.maxBatchBytes(),
                        "max-inflight-bytes", limits.maxInflightBytes(), "max-write-streams", limits.maxWriteStreams(),
                        "io-threads", limits.ioThreads(), "max-queued-operations", limits.maxQueuedOperations()));
        return Files.writeString(directory.resolve("shuffle-service.yml"), new Yaml().dump(settings));
    }

    private static void stopProcess(Process child) throws InterruptedException {
        child.destroy();
        if (!child.waitFor(30, TimeUnit.SECONDS)) {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
            throw new IllegalStateException("Shuffle process did not exit gracefully: pid=" + child.pid());
        }
    }

    private static ShuffleServiceConfig config(Path directory, ProcessSecurityAuthority authority,
            TestTlsAuthority tls, int port, URI endpoint, String bucket) throws Exception {
        var identity = tls.issue("local-shuffle", HOST, false);
        var passwordFile = Files.writeString(directory.resolve("shuffle-database-password"),
                authority.dataSource().getPassword());
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        var capabilityFile = Files.write(directory.resolve("shuffle-capability-key"), key);
        var route = new ShuffleServiceConfig.Endpoint(HOST, port, HOST, 0, -1);
        return new ShuffleServiceConfig(new ShuffleServiceConfig.Server(port, HOST, HOST, List.of(route)),
                new ShuffleServiceConfig.Database(authority.hostJdbcUrl(), authority.dataSource().getUser(),
                        passwordFile, 4),
                new ShuffleServiceConfig.ObjectStorage(bucket, "query", "us-east-1", endpoint, true, false),
                new ShuffleServiceConfig.Security(identity.certificateChain(), identity.privateKey(), tls.root(),
                        capabilityFile, List.of(COORDINATOR_IDENTITY), Duration.ofHours(1)),
                new ShuffleServiceConfig.Limits(1024 * 1024, 16 * 1024 * 1024,
                        48 * 1024 * 1024, 16, 4, 32));
    }

    String endpoint() {
        return HOST + ":" + port;
    }

    boolean objectsReleased() {
        return objects.listObjectsV2(request -> request.bucket(bucket).maxKeys(1)).contents().isEmpty();
    }

    @Override
    public void close() throws Exception {
        try (minio; objects; runtime) {
            // Stop services before releasing the object storage fixture.
        } finally {
            restoreCredentials(previousAccess, previousSecret);
        }
    }

    private static void restoreCredentials(String previousAccess, String previousSecret) {
        if (previousAccess == null) System.clearProperty(ACCESS_PROPERTY);
        else System.setProperty(ACCESS_PROPERTY, previousAccess);
        if (previousSecret == null) System.clearProperty(SECRET_PROPERTY);
        else System.setProperty(SECRET_PROPERTY, previousSecret);
    }
}
