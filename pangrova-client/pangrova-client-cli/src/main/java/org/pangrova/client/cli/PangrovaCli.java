package org.pangrova.client.cli;

import org.pangrova.client.rest.PangrovaRestClient;
import org.pangrova.client.rest.auth.ClientAuthentication;
import org.pangrova.client.rest.PangrovaRestClientConfig.TlsSettings;
import org.pangrova.transport.security.ResultTransportSecurity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Entry point for the Pangrova command-line SQL shell.
 *
 * <p>Usage:
 * <pre>
 *   java -jar pangrova-client-cli.jar --host localhost --port 8080 --catalog default --query-timeout 30m
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
            run(args, reader, System.out);
        }
    }

    /** Runs the same shell over caller-owned console streams, without replacing global process streams. */
    public static void run(String[] args, BufferedReader reader, java.io.PrintStream out) {
        CliArgs parsed = parseArgs(args);
        var builder = PangrovaRestClient.builder()
                .coordinatorHost(parsed.host)
                .coordinatorPort(parsed.port)
                .useTls(parsed.tls.enabled())
                .trustStorePath(parsed.tls.storePath())
                .trustStorePassword(parsed.tls.storePassword())
                .resultTransportSecurity(parsed.resultTransportSecurity);
        if (parsed.authentication != null) builder.authentication(parsed.authentication);
        try (PangrovaRestClient client = builder.build()) {

            if (parsed.catalog != null) {
                out.println("Catalog: " + parsed.catalog);
            }

            SqlShell repl = new SqlShell(
                    QueryClient.wrap(client), reader, out, parsed.queryTimeout,
                    parsed.catalog);
            repl.run();
        }
    }

    /**
     * Parses command-line arguments: {@code --host}, {@code --port}, {@code --catalog},
     * {@code --query-timeout}.
     * Unknown flags are silently ignored.
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
                parseResultSecurity(params));
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
        if (basic && params.containsKey("--token")) {
            throw new IllegalArgumentException("Bearer token and Basic credentials are mutually exclusive");
        }
        if (basic) return ClientAuthentication.basic(params.get("--user"), params.get("--password"));
        return params.containsKey("--token") ? ClientAuthentication.bearer(params.get("--token")) : null;
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
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                if (i + 1 >= args.length) throw new IllegalArgumentException("Missing value for CLI option " + args[i]);
                params.put(args[i], args[i + 1]);
                i++;
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

        CliArgs(String host, int port, String catalog, Duration queryTimeout,
                ClientAuthentication authentication, TlsSettings tls, ResultTransportSecurity resultTransportSecurity) {
            this.host = host;
            this.port = port;
            this.catalog = catalog;
            this.queryTimeout = queryTimeout;
            this.authentication = authentication;
            this.tls = tls;
            this.resultTransportSecurity = resultTransportSecurity;
        }
    }
}
