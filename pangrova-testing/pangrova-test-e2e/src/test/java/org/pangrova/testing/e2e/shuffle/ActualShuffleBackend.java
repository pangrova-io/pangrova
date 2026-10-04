package org.pangrova.testing.e2e.shuffle;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.pangrova.metadata.coordination.JdbcExecutionAnchorStore;
import org.pangrova.metadata.store.SchemaMigrationRunner;
import org.pangrova.shuffle.deployment.ShuffleServiceConfig;
import org.pangrova.testing.e2e.SharedTestContainers;
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
import java.sql.DriverManager;
import java.time.Duration;
import java.util.UUID;

/** Real PostgreSQL migrations, execution leases and immutable MinIO backend. */
final class ActualShuffleBackend implements AutoCloseable {
    static final String CREDENTIAL_LOCK = "pangrova-shuffle-acceptance-aws-properties";
    private static final String ACCESS_KEY = "pangrova-shuffle-test";
    private static final String SECRET_KEY = "pangrova-shuffle-test-secret";
    private final String database = "shuffle_acceptance_" + UUID.randomUUID().toString().replace("-", "");
    private final String bucket = "shuffle-acceptance-" + UUID.randomUUID();
    private final String previousAccess = System.getProperty("aws.accessKeyId");
    private final String previousSecret = System.getProperty("aws.secretAccessKey");
    private final Path directory;
    private HikariDataSource jdbc;
    private S3Client objects;
    private boolean databaseCreated;

    ActualShuffleBackend(Path directory) throws Exception {
        this.directory = directory;
        if (!SharedTestContainers.AVAILABLE) {
            throw new IllegalStateException("Actual shuffle acceptance requires PostgreSQL TestContainer");
        }
        try {
            createDatabase();
            initializeMetadata();
            initializeObjects();
            writeSecrets();
        } catch (Exception | Error failure) {
            try { close(); } catch (Exception | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private void createDatabase() throws Exception {
        adminSql("CREATE DATABASE " + database);
        databaseCreated = true;
        Files.createDirectories(directory);
    }

    private void initializeMetadata() {
        var pg = SharedTestContainers.POSTGRES;
        var config = new HikariConfig();
        config.setJdbcUrl(pg.getJdbcUrl().replaceFirst("/[^/?]+(?=\\?|$)", "/" + database));
        config.setUsername(pg.getUsername());
        config.setPassword(pg.getPassword());
        config.setMaximumPoolSize(4);
        jdbc = new HikariDataSource(config);
        SchemaMigrationRunner.migrate(jdbc);
    }

    private void initializeObjects() {
        objects = S3Client.builder().region(Region.US_EAST_1).endpointOverride(endpoint())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build()).build();
        objects.createBucket(request -> request.bucket(bucket));
        System.setProperty("aws.accessKeyId", ACCESS_KEY);
        System.setProperty("aws.secretAccessKey", SECRET_KEY);
    }

    private void writeSecrets() throws Exception {
        Files.writeString(directory.resolve("postgres-password"), SharedTestContainers.POSTGRES.getPassword());
        byte[] secret = new byte[32];
        new SecureRandom().nextBytes(secret);
        Files.write(directory.resolve("capability-secret"), secret);
        for (String name : new String[]{"localhost-cert.pem", "localhost-key.pem"}) {
            try (var input = getClass().getResourceAsStream("/shuffle-fixture/tls/" + name)) {
                if (input == null) throw new IllegalStateException("Missing actual mTLS test material: " + name);
                Files.copy(input, directory.resolve(name));
            }
        }
    }

    HikariDataSource jdbc() { return jdbc; }
    JdbcExecutionAnchorStore anchors() { return new JdbcExecutionAnchorStore(jdbc); }
    Path certificate() { return directory.resolve("localhost-cert.pem"); }
    Path privateKey() { return directory.resolve("localhost-key.pem"); }

    ShuffleServiceConfig.Database databaseConfig() {
        return new ShuffleServiceConfig.Database(jdbc.getJdbcUrl(), jdbc.getUsername(),
                directory.resolve("postgres-password"), 4);
    }

    ShuffleServiceConfig.ObjectStorage objectConfig() {
        return new ShuffleServiceConfig.ObjectStorage(bucket, "acceptance", "us-east-1", endpoint(),
                true, false, Duration.ofSeconds(30), Duration.ofSeconds(10));
    }

    ShuffleServiceConfig.Security securityConfig() {
        return new ShuffleServiceConfig.Security(certificate(), privateKey(), certificate(),
                directory.resolve("capability-secret"), java.util.List.of("localhost"), Duration.ofMinutes(5));
    }

    private static URI endpoint() {
        return URI.create("http://" + ObjectContainer.MINIO.getHost() + ":"
                + ObjectContainer.MINIO.getMappedPort(9000));
    }

    private static void adminSql(String sql) throws Exception {
        var pg = SharedTestContainers.POSTGRES;
        try (var connection = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
             var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        for (AutoCloseable owner : new AutoCloseable[]{objects, jdbc}) {
            if (owner == null) continue;
            try { owner.close(); } catch (Exception cleanup) {
                if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
            }
        }
        try {
            if (databaseCreated) {
                adminSql("DROP DATABASE " + database);
                databaseCreated = false;
            }
        } catch (Exception cleanup) {
            if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
        }
        restore("aws.accessKeyId", previousAccess);
        restore("aws.secretAccessKey", previousSecret);
        if (failure != null) throw failure;
    }

    private static void restore(String name, String previous) {
        if (previous == null) System.clearProperty(name); else System.setProperty(name, previous);
    }

    private static final class ObjectContainer {
        private static final GenericContainer<?> MINIO = start();

        private static GenericContainer<?> start() {
            var container = new GenericContainer<>("minio/minio:RELEASE.2024-12-18T13-15-44Z")
                    .withNetwork(SharedTestContainers.NETWORK).withExposedPorts(9000)
                    .withEnv("MINIO_ROOT_USER", ACCESS_KEY).withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                    .withCommand("server", "/data")
                    .waitingFor(Wait.forHttp("/minio/health/ready").forPort(9000));
            container.start();
            Runtime.getRuntime().addShutdownHook(new Thread(container::stop, "shuffle-minio-owner-shutdown"));
            return container;
        }
    }
}
