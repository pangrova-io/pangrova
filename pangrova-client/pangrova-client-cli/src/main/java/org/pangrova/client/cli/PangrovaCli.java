package org.pangrova.client.cli;

import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.client.http.authentication.ClientAuthentication;
import org.pangrova.client.http.PangrovaRestClientConfig.TlsSettings;
import org.pangrova.transport.client.security.ResultTransportSecurity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Entry point for the Pangrova command-line SQL shell.
 *
 * <p>Usage:
 * <pre>
 *   java -jar pangrova-client-cli-0.1.0-SNAPSHOT-exec.jar --host localhost --port 8080
 * </pre>
 */
public final class PangrovaCli {

    static final String DEFAULT_HOST = "localhost";
    static final int DEFAULT_PORT = 8080;
    static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofMinutes(30);

    /** Seconds in one hour, for structured timeout parsing. */
    private static final long SECONDS_PER_HOUR = 3600;
    /** Seconds in one minute, for structured timeout parsing. */
    private static final long SECONDS_PER_MINUTE = 60;
    /** Highest valid TCP port number. */
    private static final int MAX_PORT = 65535;

    private static final Pattern DURATION_PATTERN = Pattern.compile(
            "^(?:(\\d+)h)?(?:(\\d+)m)?(?:(\\d+)s)?$");

    private PangrovaCli() {}

    /** Runs the interactive SQL shell with connection settings parsed from {@code args}. */
    public static void main(String[] args) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            run(args, reader, System.out, TerminalStyle.detect());
        }
    }

    /** Runs the same shell over caller-owned console streams, without replacing global process streams. */
    public static void run(String[] args, BufferedReader reader, java.io.PrintStream out) {
        run(args, reader, out, TerminalStyle.plain());
    }

    private static void run(String[] args, BufferedReader reader, java.io.PrintStream out, TerminalStyle style) {
        CliArgs parsed = parseArgs(args);
        if (parsed.batch.file() != null) {
            runFile(parsed, reader, out);
            return;
        }
        try (PangrovaRestClient client = createClient(parsed)) {
            new SqlShell(client, reader, out, parsed.queryTimeout,
                    parsed.catalog, parsed.schema, style).run();
        }
    }

    private static PangrovaRestClient createClient(CliArgs parsed) {
        var builder = PangrovaRestClient.builder()
                .coordinatorHost(parsed.host)
                .coordinatorPort(parsed.port)
                .useTls(parsed.tls.enabled())
                .trustStorePath(parsed.tls.storePath())
                .trustStorePassword(parsed.tls.storePassword())
                .resultTransportSecurity(parsed.resultTransportSecurity);
        if (parsed.authentication != null) builder.authentication(parsed.authentication);
        return builder.build();
    }

    private static void runFile(CliArgs parsed, BufferedReader reader, java.io.PrintStream out) {
        try {
            String sql = Files.readString(parsed.batch.file(), StandardCharsets.UTF_8);
            if (sql.isBlank()) throw new IllegalArgumentException("SQL file must not be blank");
            if (parsed.batch.receipt() != null && Files.exists(parsed.batch.receipt())) {
                throw new IllegalArgumentException("Execution receipt already exists");
            }
            String queryId;
            try (PangrovaRestClient client = createClient(parsed)) {
                if (parsed.batch.output() == null) {
                    queryId = new SqlShell(client, reader, out, parsed.queryTimeout,
                            parsed.catalog, parsed.schema).execute(sql);
                } else {
                    queryId = executeToFile(parsed, client, reader, sql);
                }
            }
            if (parsed.batch.receipt() != null) {
                String receipt = org.pangrova.common.json.JsonMapperFactory
                        .writer().writeValueAsString(
                        java.util.Map.of("queryId", queryId, "state", "FINISHED", "sqlSha256",
                                org.pangrova.common.crypto.Sha256Digests.sha256Hex(sql), "resultConsumed", true));
                Files.writeString(parsed.batch.receipt(), receipt, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            }
        } catch (IOException failure) {
            throw new java.io.UncheckedIOException("Batch SQL file or result output failed", failure);
        }
    }

    private static String executeToFile(CliArgs parsed, PangrovaRestClient client,
                                       BufferedReader reader, String sql) throws IOException {
        var result = new java.io.PrintStream(Files.newOutputStream(parsed.batch.output(),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), false, StandardCharsets.UTF_8);
        String queryId;
        try (result) {
            queryId = new SqlShell(client, reader, result, parsed.queryTimeout,
                    parsed.catalog, parsed.schema).execute(sql);
        }
        if (result.checkError()) throw new IOException("Failed to close query result output");
        return queryId;
    }

    /**
     * Parses command-line arguments: {@code --host}, {@code --port}, {@code --catalog},
     * {@code --query-timeout}.
     * Unknown, duplicate and incomplete options are rejected before submitting SQL.
     */
    static CliArgs parseArgs(String[] args) {
        java.util.Map<String, String> params = extractParams(args);
        String host = params.getOrDefault("--host", DEFAULT_HOST);
        int port = params.containsKey("--port") ? parsePort(params.get("--port")) : DEFAULT_PORT;
        String catalog = params.get("--catalog");
        Duration queryTimeout = params.containsKey("--query-timeout")
                ? parseTimeout(params.get("--query-timeout"))
                : DEFAULT_QUERY_TIMEOUT;
        return new CliArgs(host, port, catalog, queryTimeout, parseAuthentication(params), parseTls(params),
                parseResultSecurity(params), params.get("--schema"), new BatchFiles(materialPath(params.get("--file")),
                materialPath(params.get("--output")), materialPath(params.get("--execution-receipt"))));
    }

    private static ResultTransportSecurity parseResultSecurity(java.util.Map<String, String> params) {
        var allowed = java.util.Set.of(
                "--result-tls-root-ca", "--result-tls-certificate-chain", "--result-tls-private-key");
        for (String option : params.keySet()) {
            if (option.startsWith("--result-tls-") && !allowed.contains(option)) {
                throw new IllegalArgumentException("Unknown result transport security option: " + option);
            }
        }
        return new ResultTransportSecurity(false, materialPath(params.get("--result-tls-root-ca")),
                materialPath(params.get("--result-tls-certificate-chain")),
                materialPath(params.get("--result-tls-private-key")));
    }

    private static java.nio.file.Path materialPath(String value) {
        return value == null ? null : java.nio.file.Path.of(value);
    }

    private static TlsSettings parseTls(java.util.Map<String, String> params) {
        String enabled = params.getOrDefault("--ssl", "false");
        if (!"true".equalsIgnoreCase(enabled) && !"false".equalsIgnoreCase(enabled)) {
            throw new IllegalArgumentException("CLI --ssl must be true or false");
        }
        return new TlsSettings(Boolean.parseBoolean(enabled), params.get("--ssl-trust-store"),
                params.get("--ssl-trust-store-password"));
    }

    private static ClientAuthentication parseAuthentication(java.util.Map<String, String> params) {
        boolean basic = params.containsKey("--user") || params.containsKey("--password");
        boolean token = params.containsKey("--token");
        boolean tokenEnvironment = params.containsKey("--token-env");
        if (token && tokenEnvironment) {
            throw new IllegalArgumentException("Bearer token sources are mutually exclusive");
        }
        if (basic && (token || tokenEnvironment)) {
            throw new IllegalArgumentException("Bearer token and Basic credentials are mutually exclusive");
        }
        if (basic) return ClientAuthentication.basic(params.get("--user"), params.get("--password"));
        if (tokenEnvironment) {
            String name = params.get("--token-env");
            if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                throw new IllegalArgumentException("Bearer token environment variable name is invalid");
            }
            String value = System.getenv(name);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Bearer token environment variable is unset or blank");
            }
            return ClientAuthentication.bearer(value);
        }
        return token ? ClientAuthentication.bearer(params.get("--token")) : null;
    }

    /**
     * Parses a human-readable duration string into a {@link Duration}.
     * Supported formats: bare seconds ({@code 90}), or combinations of hours, minutes, and
     * seconds ({@code 30s}, {@code 5m}, {@code 2h}, {@code 1h30m}, {@code 1h0m30s}).
     *
     * @throws IllegalArgumentException if the input is blank, negative, zero, or malformed
     */
    static Duration parseTimeout(String input) {
        if (input == null || input.isBlank()) {
            throw new IllegalArgumentException("Query timeout must not be blank");
        }

        // Try bare number (interpreted as seconds)
        if (input.matches("^-?\\d+$")) {
            return parseBareSeconds(input);
        }
        return parseStructuredTimeout(input);
    }

    private static Duration parseBareSeconds(String input) {
        long seconds = Long.parseLong(input);
        if (seconds <= 0) {
            throw new IllegalArgumentException(
                    "Query timeout must be positive, got " + seconds);
        }
        return Duration.ofSeconds(seconds);
    }

    /** Parses a structured duration such as {@code 1h30m45s}. */
    private static Duration parseStructuredTimeout(String input) {
        Matcher matcher = DURATION_PATTERN.matcher(input);
        if (!isStructuredMatch(matcher)) {
            throw new IllegalArgumentException(
                    "Invalid query timeout format: " + input
                    + ". Use formats like 30s, 5m, 2h, 1h30m, or a plain number of seconds");
        }

        long hours = groupValue(matcher, 1);
        long minutes = groupValue(matcher, 2);
        long seconds = groupValue(matcher, 3);

        long totalSeconds = hours * SECONDS_PER_HOUR + minutes * SECONDS_PER_MINUTE + seconds;
        if (totalSeconds <= 0) {
            throw new IllegalArgumentException("Query timeout must be positive");
        }
        return Duration.ofSeconds(totalSeconds);
    }

    /** True when the matcher matched and at least one duration component is present. */
    private static boolean isStructuredMatch(Matcher matcher) {
        return matcher.matches()
                && (matcher.group(1) != null
                    || matcher.group(2) != null
                    || matcher.group(3) != null);
    }

    /** Returns the numeric value of a matcher group, or zero when absent. */
    private static long groupValue(Matcher matcher, int group) {
        return matcher.group(group) != null ? Long.parseLong(matcher.group(group)) : 0;
    }

    private static java.util.Map<String, String> extractParams(String[] args) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        var allowed = java.util.Set.of("--host", "--port", "--catalog", "--schema", "--query-timeout",
                "--token", "--token-env", "--user", "--password", "--ssl", "--ssl-trust-store",
                "--ssl-trust-store-password",
                "--result-tls-root-ca", "--result-tls-certificate-chain", "--result-tls-private-key",
                "--file", "--output", "--execution-receipt");
        for (int i = 0; i < args.length; i++) {
            String option = args[i];
            if (!allowed.contains(option)) throw new IllegalArgumentException("Unknown CLI option: " + option);
            if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
                throw new IllegalArgumentException("Missing value for CLI option " + option);
            }
            if (params.putIfAbsent(option, args[++i]) != null) {
                throw new IllegalArgumentException("Duplicate CLI option: " + option);
            }
        }
        if (!params.containsKey("--file") && (params.containsKey("--output")
                || params.containsKey("--execution-receipt"))) {
            throw new IllegalArgumentException("Batch output and receipt require --file");
        }
        for (String option : java.util.List.of("--file", "--output", "--execution-receipt", "--schema", "--catalog")) {
            if (params.containsKey(option) && params.get(option).isBlank()) {
                throw new IllegalArgumentException("CLI option must not be blank: " + option);
            }
        }
        return params;
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > MAX_PORT) {
                throw new IllegalArgumentException(
                        "Port must be between 1 and " + MAX_PORT + ", got " + port);
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port number: " + value, e);
        }
    }

    /**
     * Parsed CLI arguments. Immutable value holder.
     */
    static final class CliArgs {
        final String host;
        final int port;
        final String catalog; // nullable
        final Duration queryTimeout;
        final ClientAuthentication authentication;
        final TlsSettings tls;
        final ResultTransportSecurity resultTransportSecurity;
        final String schema;
        final BatchFiles batch;

        CliArgs(String host, int port, String catalog, Duration queryTimeout,
                ClientAuthentication authentication, TlsSettings tls, ResultTransportSecurity resultTransportSecurity,
                String schema, BatchFiles batch) {
            this.host = host;
            this.port = port;
            this.catalog = catalog;
            this.queryTimeout = queryTimeout;
            this.authentication = authentication;
            this.tls = tls;
            this.resultTransportSecurity = resultTransportSecurity;
            this.schema = schema;
            this.batch = batch;
        }
    }

    /** File identities are checked before any SQL submission can have side effects. */
    record BatchFiles(Path file, Path output, Path receipt) {
        BatchFiles {
            var paths = new java.util.HashSet<Path>();
            for (Path path : new Path[]{file, output, receipt}) {
                if (path != null && !paths.add(path.toAbsolutePath().normalize())) {
                    throw new IllegalArgumentException("SQL input, output and receipt must use distinct paths");
                }
            }
        }
    }
}
