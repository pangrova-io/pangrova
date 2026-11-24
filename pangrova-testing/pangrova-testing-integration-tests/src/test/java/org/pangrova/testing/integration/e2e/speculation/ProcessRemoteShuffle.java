package org.pangrova.testing.integration.e2e.speculation;

import org.pangrova.shuffle.config.ShuffleServiceConfig;
import org.pangrova.shuffle.server.ShuffleServerRuntime;
import org.pangrova.testing.testkit.MinioContainer;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/** Production shuffle service on the Coordinator's actual execution-anchor schema. */
final class ProcessRemoteShuffle implements AutoCloseable {
    static final String COORDINATOR_IDENTITY = "pangrova-coordinator.test";
    private static final String HOST = "localhost";
    private static final String ACCESS_PROPERTY = "aws.accessKeyId";
    private static final String SECRET_PROPERTY = "aws.secretAccessKey";

    private final ShuffleServerRuntime runtime;
    private final GenericContainer<?> minio;
    private final S3Client objects;
    private final String bucket;
    private final int port;
    private final String previousAccess;
    private final String previousSecret;

    private ProcessRemoteShuffle(ShuffleServerRuntime runtime, GenericContainer<?> minio,
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
            TestTlsAuthority tls, int port) throws Exception {
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
            System.setProperty(ACCESS_PROPERTY, access);
            System.setProperty(SECRET_PROPERTY, secret);
            return new ProcessRemoteShuffle(ShuffleServerRuntime.start(config), minio, objects, bucket,
                    port, previousAccess, previousSecret);
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
                        48 * 1024 * 1024, 4, 4, 16));
    }

    String endpoint() {
        return HOST + ":" + port;
    }

    boolean objectsReleased() {
        return objects.listObjectsV2(request -> request.bucket(bucket).maxKeys(1)).contents().isEmpty();
    }

    @Override
    public void close() {
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
