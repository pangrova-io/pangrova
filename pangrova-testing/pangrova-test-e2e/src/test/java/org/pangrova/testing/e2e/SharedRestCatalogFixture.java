package org.pangrova.testing.e2e;

import org.pangrova.testing.ContainerTestBase;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.MountableFile;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/** Real REST service retained alongside the shared PostgreSQL authority for the JVM lifetime. */
public final class SharedRestCatalogFixture extends ContainerTestBase {
    private static GenericContainer<?> server;
    private static URI endpoint;
    private static Throwable startupFailure;

    private SharedRestCatalogFixture() {}

    /** Starts an independent Iceberg REST provider backed by a dedicated PostgreSQL schema. */
    public static synchronized URI endpoint() {
        if (startupFailure != null) {
            throw new IllegalStateException("REST catalog fixture startup previously failed", startupFailure);
        }
        if (endpoint != null) return endpoint;
        ensureDockerAvailable();
        try {
            Path target = Files.createDirectories(Path.of("target").toAbsolutePath());
            Path directory = Files.createTempDirectory(target, "rest-catalog-fixture-");
            Path warehouse = Files.createDirectory(directory.resolve("warehouse"));
            if (Files.getFileStore(warehouse).supportsFileAttributeView("posix")) {
                Files.setPosixFilePermissions(warehouse, PosixFilePermissions.fromString("rwxrwxrwx"));
            }
            var postgres = SharedTestContainers.POSTGRES;
            String schema = "rest_fixture_" + UUID.randomUUID().toString().replace("-", "");
            try (var connection = DriverManager.getConnection(
                    postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                    var statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA " + schema);
            }
            String database = "jdbc:postgresql://" + postgres.getNetworkAliases().get(0)
                    + ":5432/" + postgres.getDatabaseName() + "?currentSchema=" + schema;
            Path configuration = directory.resolve("rest.conf");
            Files.writeString(configuration, """
                    gravitino.iceberg-rest.host=0.0.0.0
                    gravitino.iceberg-rest.httpPort=9001
                    gravitino.iceberg-rest.catalog-backend=jdbc
                    gravitino.iceberg-rest.jdbc-driver=org.postgresql.Driver
                    gravitino.iceberg-rest.jdbc-initialize=true
                    """ + "gravitino.iceberg-rest.uri=" + database + "\n"
                    + "gravitino.iceberg-rest.jdbc-user=" + postgres.getUsername() + "\n"
                    + "gravitino.iceberg-rest.jdbc-password=" + postgres.getPassword() + "\n"
                    + "gravitino.iceberg-rest.warehouse=" + warehouse.toUri() + "\n");
            Path driver = Path.of(org.postgresql.Driver.class.getProtectionDomain()
                    .getCodeSource().getLocation().toURI());
            Path logging = directory.resolve("logging.properties");
            Files.writeString(logging, """
                    status=error
                    appender.console.type=Console
                    appender.console.name=console
                    appender.console.layout.type=PatternLayout
                    appender.console.layout.pattern=%d{ISO8601} %p %c - %m%n
                    rootLogger.level=info
                    rootLogger.appenderRef.console.ref=console
                    """);
            Path launcher = directory.resolve("launch.sh");
            Files.writeString(launcher, """
                    #!/bin/bash
                    cd /opt/gravitino-iceberg-rest-server || exit 1
                    source ./bin/gravitino-iceberg-rest-server.sh status || exit 1
                    # The optional Aliyun bundle contains an old, unrelocated
                    # Jackson core. Prefer the image's published Jackson cohort.
                    for jar in "$PWD"/libs/jackson-*.jar; do
                      test -f "$jar" || exit 1
                      GRAVITINO_CLASSPATH="$jar:$GRAVITINO_CLASSPATH"
                    done
                    umask 000
                    run
                    """);
            server = new GenericContainer<>(System.getProperty(
                    "pangrova.test.rest-catalog.image", "apache/gravitino-iceberg-rest:1.3.0"))
                    .withNetwork(SharedTestContainers.NETWORK)
                    .withExposedPorts(9001)
                    .withEnv("SKIP_CONFIG_REWRITE", "true")
                    .withEnv("GRAVITINO_MEM", "-Xms64m -Xmx256m -XX:MaxMetaspaceSize=256m")
                    .withFileSystemBind(warehouse.toString(), warehouse.toString(), BindMode.READ_WRITE)
                    .withCopyFileToContainer(MountableFile.forHostPath(configuration, 0644),
                            "/opt/gravitino-iceberg-rest-server/conf/gravitino-iceberg-rest-server.conf")
                    .withCopyFileToContainer(MountableFile.forHostPath(driver, 0644),
                            "/opt/gravitino-iceberg-rest-server/libs/postgresql-fixture.jar")
                    .withCopyFileToContainer(MountableFile.forHostPath(logging, 0644),
                            "/opt/gravitino-iceberg-rest-server/conf/gravitino-iceberg-rest-log4j2.properties")
                    .withCopyFileToContainer(MountableFile.forHostPath(launcher, 0755),
                            "/opt/gravitino-iceberg-rest-server/bin/pangrova-fixture-launch.sh")
                    .withLogConsumer(new Slf4jLogConsumer(org.slf4j.LoggerFactory.getLogger(
                            SharedRestCatalogFixture.class)).withPrefix("rest-provider"))
                    .withCreateContainerCmdModifier(command -> command.withEntrypoint("/bin/bash",
                            "/opt/gravitino-iceberg-rest-server/bin/pangrova-fixture-launch.sh"))
                    .waitingFor(Wait.forHttp("/iceberg/v1/config").forPort(9001).forStatusCode(200)
                            .withStartupTimeout(Duration.ofSeconds(60)));
            server.start();
            URI started = URI.create("http://" + server.getHost() + ":" + server.getMappedPort(9001)
                    + "/iceberg");
            try (var catalog = new org.apache.iceberg.rest.RESTCatalog()) {
                catalog.initialize("rest-fixture", Map.of("uri", started.toString()));
                catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("default"));
            }
            endpoint = started;
            return endpoint;
        } catch (Exception | Error failure) {
            startupFailure = failure;
            try { closeIfStarted(); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw new IllegalStateException("Real PostgreSQL-backed REST catalog startup failed", failure);
        }
    }

    /** Called by the shared owner before PostgreSQL and its Docker network are stopped. */
    public static synchronized void closeIfStarted() {
        if (server != null) {
            server.stop();
            server = null;
            endpoint = null;
        }
    }
}
