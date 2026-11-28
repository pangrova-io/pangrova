package org.pangrova.testing.integration.e2e.client;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.pangrova.client.cli.PangrovaCli;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.setup.TestConnectorFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/** Batch CLI through the real HTTP Coordinator, packaged JNI and Arrow result channel. */
class CliBatchExecutionE2ETest extends PangrovaE2ETestBase {
    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String TOKEN_VARIABLE = "PANGROVA_TPC_E2E_TOKEN";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadRealQueryTables() {
        registerTestConnectors(List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString())));
        // The shared correctness fixture writes real Iceberg files and reuses
        // existing tables. Its row multiplier is not a calibrated TPC scale.
        dataSetup.loadTpcHDataViaConnector(TestConnectorFactory.createIcebergConnector(icebergWarehouse()), 1);
    }

    @Test
    void consumesActualSqlResultBeforeIssuingExecutionReceipt() throws Exception {
        var directory = Files.createDirectory(tempDir.resolve("cli-" + UUID.randomUUID()));
        var sql = directory.resolve("query.sql");
        var output = directory.resolve("result.txt");
        var receipt = directory.resolve("receipt.json");
        String statement = "SELECT 42 AS answer;\n";
        Files.writeString(sql, statement, StandardCharsets.UTF_8);
        run(sql, output, receipt);
        assertThat(Files.readString(output)).contains("answer", "42");
        var result = JsonMapperFactory.shared().readTree(Files.readString(receipt));
        assertThat(result.path("queryId").asText()).isNotBlank();
        assertThat(result.path("state").asText()).isEqualTo("FINISHED");
        assertThat(result.path("resultConsumed").asBoolean()).isTrue();
        assertThat(result.path("sqlSha256").asText()).isEqualTo(Sha256Digests.sha256Hex(statement));
        assertThat(pollUntilTerminal(result.path("queryId").asText())).contains("\"FINISHED\"");
    }

    @Test
    void actualSqlFailureCannotProduceSuccessReceipt() throws Exception {
        var directory = Files.createDirectory(tempDir.resolve("cli-" + UUID.randomUUID()));
        var sql = directory.resolve("query.sql");
        var receipt = directory.resolve("receipt.json");
        Files.writeString(sql, "SELECT missing_column FROM missing_table", StandardCharsets.UTF_8);
        assertThatThrownBy(() -> run(sql, directory.resolve("result.txt"), receipt))
                .isInstanceOf(RuntimeException.class);
        assertThat(receipt).doesNotExist();
    }

    @Test
    void packagedCliProcessesCompleteRealQ1BeforeRecordingRawSamples() throws Exception {
        Path root = repositoryRoot();
        Path directory = Files.createDirectory(tempDir.resolve("tpc-cli-" + UUID.randomUUID()));
        Path report = directory.resolve("samples.json");
        Path log = directory.resolve("runner.log");
        int exit = runTpcProcesses(root, directory, report, log, "e2e-user-token");
        assertThat(exit).as("real batch CLI and TPC runner exit").isZero();
        var results = JsonMapperFactory.shared().readTree(Files.readString(report));
        assertThat(results.path("status").asText()).isEqualTo("passed");
        assertThat(results.path("summary").path("passed").asInt()).isEqualTo(1);
        assertThat(results.path("summary").path("failed").asInt()).isZero();
        assertThat(results.path("percentileMethod").asText()).isEqualTo("R7");
        assertThat(results.path("timingScope").asText()).isEqualTo("cli-end-to-end");
        assertThat(results.path("config").path("timeout").asInt()).isEqualTo(45);
        assertThat(results.path("config").path("queryTimeout").asInt()).isEqualTo(30);
        assertThat(results.path("environment").path("catalog").asText()).isEqualTo(CATALOG);
        assertThat(results.path("environment").path("schema").asText()).isEqualTo(SCHEMA);
        assertThat(results.path("environment").path("authentication").asText()).isEqualTo("bearer-environment");
        var record = results.path("queries").path("q01");
        assertThat(record.path("status").asText()).isEqualTo("passed");
        assertThat(record.path("sqlSha256").asText()).isEqualTo(Sha256Digests.sha256Hex(
                Files.readString(root.resolve("pangrova-testing/pangrova-testing-benchmark/queries/tpch/q01.sql"),
                        StandardCharsets.UTF_8)));
        assertThat(record.path("warmupCompleted").asInt()).isEqualTo(1);
        assertThat(record.path("sampleCount").asInt()).isEqualTo(2);
        assertThat(record.path("minimumSamples").asInt()).isEqualTo(1000);
        assertThat(record.path("percentileQualified").asBoolean()).isFalse();
        assertThat(record.has("p99Ms")).isFalse();
        var samples = record.path("samples");
        assertThat(samples.size()).isEqualTo(2);
        double first = samples.get(0).asDouble();
        double second = samples.get(1).asDouble();
        assertThat(first).isPositive().isFinite();
        assertThat(second).isPositive().isFinite();
        assertThat(record.path("p50Ms").asDouble()).isCloseTo((first + second) / 2, within(1e-9));
        assertThat(record.path("meanMs").asDouble()).isCloseTo((first + second) / 2, within(1e-9));
        assertThat(record.path("minMs").asDouble()).isEqualTo(Math.min(first, second));
        assertThat(record.path("maxMs").asDouble()).isEqualTo(Math.max(first, second));
        assertThat(record.path("warmupQueryIds").size()).isEqualTo(1);
        assertThat(record.path("queryIds").size()).isEqualTo(2);
        var ids = new HashSet<String>();
        for (String phase : List.of("warmupQueryIds", "queryIds")) {
            for (var id : record.path(phase)) {
                assertThat(ids.add(id.asText())).as("each CLI process submits a distinct query").isTrue();
                var terminal = JsonMapperFactory.shared().readTree(pollUntilTerminal(id.asText()));
                assertThat(terminal.path("state").asText()).isEqualTo("FINISHED");
                var stateMachine = queryManager.getStateMachines().get(id.asText());
                assertThat(stateMachine).as("actual Coordinator query authority").isNotNull();
                assertThat(stateMachine.getStageStates()).as("Q1 creates actual distributed stages").isNotEmpty();
                assertThat(hasControlPlaneResult(id.asText())).as("Q1 reads Native scan results").isFalse();
                var endpoint = stateMachine.getResultEndpoint().orElseThrow();
                assertThat(endpoint.port()).as("actual packaged Native result server").isEqualTo(sharedResultServerPort);
                assertThat(endpoint.ticket()).isNotBlank();
            }
        }
        assertThat(ids).hasSize(3);
        assertSanitized(report, log, directory, "e2e-user-token");
    }

    @Test
    void realAuthenticationFailureCannotBecomeAWarmupOrLatencySample() throws Exception {
        Path root = repositoryRoot();
        Path directory = Files.createDirectory(tempDir.resolve("tpc-cli-" + UUID.randomUUID()));
        Path report = directory.resolve("samples.json");
        Path log = directory.resolve("runner.log");
        String invalidToken = "invalid-tpc-credential-" + UUID.randomUUID();
        assertThat(runTpcProcesses(root, directory, report, log, invalidToken)).isEqualTo(1);
        var results = JsonMapperFactory.shared().readTree(Files.readString(report));
        assertThat(results.path("status").asText()).isEqualTo("failed");
        assertThat(results.path("summary").path("failed").asInt()).isEqualTo(1);
        var record = results.path("queries").path("q01");
        assertThat(record.path("status").asText()).isEqualTo("failed");
        assertThat(record.path("failurePhase").asText()).isEqualTo("warmup");
        assertThat(record.path("warmupCompleted").asInt()).isZero();
        assertThat(record.path("sampleCount").asInt()).isZero();
        assertThat(record.path("samples").isEmpty()).isTrue();
        assertThat(record.path("queryIds").isEmpty()).isTrue();
        assertThat(record.path("warmupQueryIds").isEmpty()).isTrue();
        assertThat(record.path("error").asText()).contains("CLI failed exit=");
        assertThat(record.has("p99Ms")).isFalse();
        assertSanitized(report, log, directory, invalidToken);
    }

    @Test
    void catalogProbeListsSchemasInARegisteredCatalogWithoutLocalYaml() throws Exception {
        Path directory = Files.createDirectory(tempDir.resolve("catalog-probe-" + UUID.randomUUID()));
        Path log = directory.resolve("probe.log");

        assertThat(runCatalogProbe(directory, log, CATALOG, "e2e-user-token")).isZero();

        String output = Files.readString(log);
        assertThat(output).contains("schema_name", SCHEMA, "connection test passed");
        assertThat(output.contains("e2e-user-token")).isFalse();
    }

    @Test
    void catalogProbeDoesNotReportSuccessForAnUnknownCatalog() throws Exception {
        Path directory = Files.createDirectory(tempDir.resolve("catalog-probe-" + UUID.randomUUID()));
        Path log = directory.resolve("probe.log");
        String missing = "missing_catalog_" + UUID.randomUUID().toString().replace("-", "");

        assertThat(runCatalogProbe(directory, log, missing, "e2e-user-token")).isNotZero();

        assertThat(Files.readString(log)).contains(missing).doesNotContain("connection test passed");
    }

    @Test
    void catalogProbeFailsWithAnInvalidBearerToken() throws Exception {
        Path directory = Files.createDirectory(tempDir.resolve("catalog-probe-" + UUID.randomUUID()));
        Path log = directory.resolve("probe.log");
        String invalidToken = "invalid-catalog-credential-" + UUID.randomUUID();

        assertThat(runCatalogProbe(directory, log, CATALOG, invalidToken)).isNotZero();

        String output = Files.readString(log);
        assertThat(output).contains("invalid E2E bearer credential").doesNotContain("connection test passed");
        assertThat(output.contains(invalidToken)).isFalse();
    }

    private static int runCatalogProbe(Path config, Path log, String catalog, String token) throws Exception {
        Path root = repositoryRoot();
        var builder = new ProcessBuilder("bash", root.resolve("bin/pangrova.sh").toString(),
                "catalog", "test", catalog, "--coordinator-url",
                "http://127.0.0.1:" + coordinatorHttpServer.getPort(), "--token-env", TOKEN_VARIABLE)
                .directory(root.toFile()).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().put(TOKEN_VARIABLE, token);
        builder.environment().put("JAVA_HOME", System.getProperty("java.home"));
        builder.environment().put("PANGROVA_HOME", root.toString());
        builder.environment().put("PANGROVA_CONF_DIR", config.toString());
        Process process = builder.start();
        process.getOutputStream().close();
        return awaitExit(process, "catalog probe", 45);
    }

    private static int runTpcProcesses(Path root, Path directory, Path report, Path log, String token)
            throws Exception {
        Path launcher = root.resolve("bin/pangrova-cli.sh");
        assertThat(Files.isExecutable(launcher)).as("production CLI launcher").isTrue();
        // A missing packaged artifact is an infrastructure failure, never a skip
        // or a switch to a test-only shell/SQL implementation.
        try (var jars = Files.list(root.resolve("pangrova-client/pangrova-client-cli/target"))) {
            assertThat(jars.anyMatch(path -> path.getFileName().toString().matches("pangrova-client-cli-.*-exec\\.jar")))
                    .as("current reactor must package the real CLI before E2E").isTrue();
        }
        var builder = new ProcessBuilder("bash",
                root.resolve("pangrova-testing/pangrova-testing-benchmark/scripts/run-tpc-benchmark.sh").toString(),
                "--benchmark", "tpch", "--queries", "q01", "--scale-factor", "1",
                "--queries-dir", root.resolve("pangrova-testing/pangrova-testing-benchmark/queries").toString(),
                "--host", "localhost", "--port", Integer.toString(coordinatorHttpServer.getPort()),
                "--catalog", CATALOG, "--schema", SCHEMA, "--cli-path", launcher.toString(),
                "--token-env", TOKEN_VARIABLE, "--warmup", "1", "--iterations", "2",
                "--timeout", "45", "--query-timeout", "30", "--output", report.toString())
                .directory(root.toFile()).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().put(TOKEN_VARIABLE, token);
        builder.environment().put("JAVA_HOME", System.getProperty("java.home"));
        builder.environment().put("PANGROVA_HOME", root.toString());
        builder.environment().put("PANGROVA_CONF_DIR", directory.toString());
        builder.environment().remove("PANGROVA_COORDINATOR_URL");
        Process process = builder.start();
        process.getOutputStream().close();
        return awaitExit(process, "TPC runner", 150);
    }

    private static int awaitExit(Process process, String owner, int timeoutSeconds) throws Exception {
        try {
            assertThat(process.waitFor(timeoutSeconds, TimeUnit.SECONDS))
                    .as("%s process completed", owner).isTrue();
            return process.exitValue();
        } finally {
            if (process.isAlive()) {
                var descendants = process.descendants().toList();
                descendants.forEach(ProcessHandle::destroyForcibly);
                process.destroyForcibly();
                assertThat(process.waitFor(10, TimeUnit.SECONDS)).as("%s process reaped", owner).isTrue();
                for (var child : descendants) {
                    child.onExit().get(10, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static void assertSanitized(Path report, Path log, Path directory, String token) throws Exception {
        for (Path file : List.of(report, log)) {
            String content = Files.readString(file);
            // Boolean assertions do not repeat a leaked secret in failure output.
            assertThat(content.contains(token)).as("credential absent from report/log").isFalse();
            assertThat(content.contains(directory.toString())).as("local paths absent from report/log").isFalse();
        }
    }

    private static Path repositoryRoot() {
        Path candidate = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (candidate != null) {
            if (Files.isRegularFile(candidate.resolve("pom.xml"))
                    && Files.isRegularFile(candidate.resolve("bin/pangrova-cli.sh"))) {
                return candidate;
            }
            candidate = candidate.getParent();
        }
        throw new IllegalStateException("Run CLI/TPC E2E from the Pangrova Maven reactor");
    }

    private static void run(Path sql, Path output, Path receipt) {
        PangrovaCli.run(new String[]{"--host", "localhost", "--port", Integer.toString(coordinatorHttpServer.getPort()),
                "--token", "e2e-user-token", "--file", sql.toString(), "--output", output.toString(),
                "--execution-receipt", receipt.toString(), "--query-timeout", "30s"},
                new BufferedReader(new StringReader("")),
                new PrintStream(new ByteArrayOutputStream(), false, StandardCharsets.UTF_8));
    }
}
