package org.pangrova.shuffle.deployment;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/** Immutable deployment configuration for one shuffle-service process. */
public record ShuffleServiceConfig(
        Server server,
        Database database,
        ObjectStorage objectStorage,
        Security security,
        Limits limits) {

    private static final int DEFAULT_CAPABILITY_TTL_SECONDS = 3600;
    private static final int DEFAULT_MAX_CHUNK_BYTES = 1024 * 1024;
    private static final int DEFAULT_MAX_BATCH_BYTES = 256 * 1024 * 1024;
    private static final int DEFAULT_MAX_INFLIGHT_BYTES = 768 * 1024 * 1024;
    private static final int DEFAULT_MAX_WRITE_STREAMS = 64;
    private static final int DEFAULT_IO_THREADS = 32;
    private static final int DEFAULT_MAX_QUEUED_OPERATIONS = 1024;
    private static final int DEFAULT_API_CALL_TIMEOUT_SECONDS = 120;
    private static final int DEFAULT_API_CALL_ATTEMPT_TIMEOUT_SECONDS = 45;
    private static final int MAX_PORT = 65_535;

    public ShuffleServiceConfig {
        require(server, "server");
        require(database, "database");
        require(objectStorage, "objectStorage");
        require(security, "security");
        require(limits, "limits");
    }

    /** Loads a strict YAML configuration file. */
    public static ShuffleServiceConfig load(Path path) throws IOException {
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException("configuration must be a readable YAML file");
        }
        Map<String, Object> root;
        try (InputStream input = Files.newInputStream(path)) {
            root = new Yaml(new SafeConstructor(new LoaderOptions())).load(input);
        }
        if (root == null) {
            throw new IllegalArgumentException("configuration must not be empty");
        }
        return from(root);
    }

    static ShuffleServiceConfig from(Map<String, Object> root) {
        Map<String, Object> server = section(root, "server");
        Map<String, Object> database = section(root, "database");
        Map<String, Object> objectStorage = section(root, "object-storage");
        Map<String, Object> security = section(root, "security");
        Map<String, Object> limits = optionalSection(root, "limits");
        validateSections(root, server, database, objectStorage, security, limits);
        return build(server, database, objectStorage, security, limits);
    }

    private static void validateSections(
            Map<String, Object> root,
            Map<String, Object> server,
            Map<String, Object> database,
            Map<String, Object> objectStorage,
            Map<String, Object> security,
            Map<String, Object> limits) {
        rejectUnknown(root, Set.of(
                "server", "database", "object-storage", "security", "limits"), "root");
        rejectUnknown(server, Set.of(
                "port", "advertised-host", "tls-identity", "endpoints"), "server");
        rejectUnknown(database, Set.of(
                "jdbc-url", "username", "password-file", "maximum-pool-size"), "database");
        rejectUnknown(objectStorage, Set.of(
                "bucket", "prefix", "region", "endpoint", "path-style-access",
                "replication-enabled", "api-call-timeout-seconds",
                "api-call-attempt-timeout-seconds"), "object-storage");
        rejectUnknown(security, Set.of(
                "certificate-chain-path", "private-key-path", "root-ca-path",
                "capability-secret-path", "capability-ttl-seconds",
                "coordinator-tls-identities"), "security");
        rejectUnknown(limits, Set.of(
                "max-chunk-bytes", "max-batch-bytes", "max-inflight-bytes",
                "max-write-streams", "io-threads", "max-queued-operations"), "limits");
    }

    private static ShuffleServiceConfig build(
            Map<String, Object> server,
            Map<String, Object> database,
            Map<String, Object> objectStorage,
            Map<String, Object> security,
            Map<String, Object> limits) {
        int port = integer(server, "port");
        String host = string(server, "advertised-host");
        String tlsIdentity = string(server, "tls-identity");
        List<Endpoint> endpoints = endpoints(server, host, port, tlsIdentity);
        return new ShuffleServiceConfig(
                new Server(port, host, tlsIdentity, endpoints),
                new Database(
                        string(database, "jdbc-url"),
                        string(database, "username"),
                        Path.of(string(database, "password-file")),
                        optionalInteger(database, "maximum-pool-size", 32)),
                new ObjectStorage(
                        string(objectStorage, "bucket"),
                        optionalString(objectStorage, "prefix", "pangrova/shuffle"),
                        optionalString(objectStorage, "region", "us-east-1"),
                        optionalUri(objectStorage, "endpoint"),
                        optionalBoolean(objectStorage, "path-style-access", false),
                        optionalBoolean(objectStorage, "replication-enabled", false),
                        Duration.ofSeconds(optionalInteger(
                                objectStorage, "api-call-timeout-seconds",
                                DEFAULT_API_CALL_TIMEOUT_SECONDS)),
                        Duration.ofSeconds(optionalInteger(
                                objectStorage, "api-call-attempt-timeout-seconds",
                                DEFAULT_API_CALL_ATTEMPT_TIMEOUT_SECONDS))),
                new Security(
                        Path.of(string(security, "certificate-chain-path")),
                        Path.of(string(security, "private-key-path")),
                        Path.of(string(security, "root-ca-path")),
                        Path.of(string(security, "capability-secret-path")),
                        stringList(security, "coordinator-tls-identities"),
                        Duration.ofSeconds(optionalInteger(
                                security, "capability-ttl-seconds",
                                DEFAULT_CAPABILITY_TTL_SECONDS))),
                new Limits(
                        optionalInteger(limits, "max-chunk-bytes", DEFAULT_MAX_CHUNK_BYTES),
                        optionalInteger(limits, "max-batch-bytes", DEFAULT_MAX_BATCH_BYTES),
                        optionalInteger(limits, "max-inflight-bytes", DEFAULT_MAX_INFLIGHT_BYTES),
                        optionalInteger(limits, "max-write-streams", DEFAULT_MAX_WRITE_STREAMS),
                        optionalInteger(limits, "io-threads", DEFAULT_IO_THREADS),
                        optionalInteger(limits, "max-queued-operations",
                                DEFAULT_MAX_QUEUED_OPERATIONS)));
    }

    private static List<Endpoint> endpoints(
            Map<String, Object> server,
            String host,
            int port,
            String tlsIdentity) {
        Object configured = server.get("endpoints");
        if (configured == null) {
            return List.of(new Endpoint(host, port, tlsIdentity, 0, -1));
        }
        if (!(configured instanceof List<?> entries) || entries.isEmpty()) {
            throw new IllegalArgumentException("server.endpoints must be a non-empty list");
        }
        List<Endpoint> result = new ArrayList<>(entries.size());
        for (Object entry : entries) {
            if (!(entry instanceof Map<?, ?> raw)) {
                throw new IllegalArgumentException("server endpoint must be a mapping");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> endpoint = (Map<String, Object>) raw;
            rejectUnknown(endpoint, Set.of(
                    "host", "port", "tls-identity", "partition-start", "partition-end"),
                    "server.endpoints");
            result.add(new Endpoint(
                    string(endpoint, "host"), integer(endpoint, "port"),
                    string(endpoint, "tls-identity"),
                    integer(endpoint, "partition-start"),
                    integer(endpoint, "partition-end")));
        }
        return List.copyOf(result);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> section(Map<String, Object> root, String name) {
        Object value = root.get(name);
        if (!(value instanceof Map<?, ?>)) {
            throw new IllegalArgumentException(name + " configuration is required");
        }
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> optionalSection(
            Map<String, Object> root,
            String name) {
        Object value = root.get(name);
        if (value == null) {
            return Map.of();
        }
        if (!(value instanceof Map<?, ?>)) {
            throw new IllegalArgumentException(name + " configuration must be a mapping");
        }
        return (Map<String, Object>) value;
    }

    private static String string(Map<String, Object> values, String name) {
        String value = optionalString(values, name, "");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static String optionalString(
            Map<String, Object> values,
            String name,
            String defaultValue) {
        Object value = values.get(name);
        return value == null ? defaultValue : value.toString().trim();
    }

    private static int integer(Map<String, Object> values, String name) {
        Object value = values.get(name);
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException(name + " must be an integer");
        }
        try {
            return new java.math.BigDecimal(number.toString()).intValueExact();
        } catch (ArithmeticException invalid) {
            throw new IllegalArgumentException(name + " must be an exact 32-bit integer", invalid);
        }
    }

    private static int optionalInteger(
            Map<String, Object> values,
            String name,
            int defaultValue) {
        return values.containsKey(name) ? integer(values, name) : defaultValue;
    }

    private static boolean optionalBoolean(
            Map<String, Object> values,
            String name,
            boolean defaultValue) {
        Object value = values.get(name);
        if (value == null) {
            return defaultValue;
        }
        if (!(value instanceof Boolean result)) {
            throw new IllegalArgumentException(name + " must be a boolean");
        }
        return result;
    }

    private static List<String> stringList(Map<String, Object> values, String name) {
        Object value = values.get(name);
        if (!(value instanceof List<?> entries) || entries.isEmpty()
                || entries.stream().anyMatch(entry ->
                        !(entry instanceof String text) || text.isBlank())) {
            throw new IllegalArgumentException(name + " must be a non-empty string list");
        }
        return entries.stream().map(Object::toString).map(String::trim).toList();
    }

    private static void rejectUnknown(
            Map<String, Object> values,
            Set<String> allowed,
            String section) {
        for (String key : values.keySet()) {
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException(
                        section + " contains unknown key: " + key);
            }
        }
    }

    private static URI optionalUri(Map<String, Object> values, String name) {
        String value = optionalString(values, name, "");
        return value.isBlank() ? null : URI.create(value);
    }

    private static void require(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }

    /** gRPC listener and advertised partition routes. */
    public record Server(
            int port,
            String advertisedHost,
            String tlsIdentity,
            List<Endpoint> endpoints) {
        public Server {
            if (!validServer(port, advertisedHost, tlsIdentity, endpoints)) {
                throw new IllegalArgumentException("invalid shuffle server configuration");
            }
            endpoints = List.copyOf(endpoints);
        }
    }

    private static boolean validServer(
            int port,
            String host,
            String identity,
            List<Endpoint> endpoints) {
        return port > 0 && port <= MAX_PORT && host != null && !host.isBlank()
                && identity != null && !identity.isBlank()
                && endpoints != null && !endpoints.isEmpty();
    }

    /** One advertised half-open partition route; end -1 expands to allocation size. */
    public record Endpoint(
            String host,
            int port,
            String tlsIdentity,
            int partitionStart,
            int partitionEnd) {
        public Endpoint {
            if (!validEndpointIdentity(host, port, tlsIdentity)
                    || !validEndpointRange(partitionStart, partitionEnd)) {
                throw new IllegalArgumentException("invalid advertised shuffle endpoint");
            }
        }
    }

    private static boolean validEndpointIdentity(String host, int port, String identity) {
        return host != null && !host.isBlank() && port > 0 && port <= MAX_PORT
                && identity != null && !identity.isBlank();
    }

    private static boolean validEndpointRange(int start, int end) {
        return start >= 0 && end != 0 && end >= -1
                && (end == -1 || end > start);
    }

    /** PostgreSQL connection configuration; the password is read from a mounted file. */
    public record Database(
            String jdbcUrl,
            String username,
            Path passwordFile,
            int maximumPoolSize) {
        public Database {
            if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:postgresql:")
                    || username == null || username.isBlank()
                    || passwordFile == null || maximumPoolSize <= 0) {
                throw new IllegalArgumentException("invalid PostgreSQL configuration");
            }
        }
    }

    /** S3-compatible immutable object storage configuration. */
    public record ObjectStorage(
            String bucket,
            String prefix,
            String region,
            URI endpoint,
            boolean pathStyleAccess,
            boolean replicationEnabled,
            Duration apiCallTimeout,
            Duration apiCallAttemptTimeout) {
        public ObjectStorage(String bucket, String prefix, String region, URI endpoint,
                boolean pathStyleAccess, boolean replicationEnabled) {
            this(bucket, prefix, region, endpoint, pathStyleAccess, replicationEnabled,
                    Duration.ofSeconds(DEFAULT_API_CALL_TIMEOUT_SECONDS),
                    Duration.ofSeconds(DEFAULT_API_CALL_ATTEMPT_TIMEOUT_SECONDS));
        }

        public ObjectStorage {
            if (bucket == null || bucket.isBlank() || prefix == null || prefix.isBlank()
                    || region == null || region.isBlank()) {
                throw new IllegalArgumentException("invalid object storage configuration");
            }
            if (apiCallTimeout == null || apiCallAttemptTimeout == null
                    || apiCallTimeout.isZero() || apiCallTimeout.isNegative()
                    || apiCallAttemptTimeout.isZero() || apiCallAttemptTimeout.isNegative()
                    || apiCallAttemptTimeout.compareTo(apiCallTimeout) > 0) {
                throw new IllegalArgumentException("invalid object storage API timeouts");
            }
            prefix = prefix.replaceAll("^/+|/+$", "");
        }
    }

    /** Mounted TLS identity and HMAC capability key. */
    public record Security(
            Path certificateChain,
            Path privateKey,
            Path rootCa,
            Path capabilitySecret,
            List<String> coordinatorTlsIdentities,
            Duration capabilityTtl) {
        public Security {
            if (!validSecurityFiles(certificateChain, privateKey, rootCa, capabilitySecret)
                    || !validSecurityPolicy(coordinatorTlsIdentities, capabilityTtl)) {
                throw new IllegalArgumentException("invalid security configuration");
            }
            coordinatorTlsIdentities = List.copyOf(coordinatorTlsIdentities);
        }
    }

    private static boolean validSecurityFiles(
            Path certificate,
            Path key,
            Path rootCa,
            Path capabilitySecret) {
        return certificate != null && key != null && rootCa != null && capabilitySecret != null;
    }

    private static boolean validSecurityPolicy(List<String> identities, Duration ttl) {
        return identities != null && !identities.isEmpty() && ttl != null
                && !ttl.isNegative() && !ttl.isZero();
    }

    /** Bounded wire and reassembly limits. */
    public record Limits(
            int maxChunkBytes,
            int maxBatchBytes,
            int maxInflightBytes,
            int maxWriteStreams,
            int ioThreads,
            int maxQueuedOperations) {
        public Limits {
            if (maxChunkBytes <= 0 || maxBatchBytes < maxChunkBytes
                    || maxInflightBytes < 3L * maxBatchBytes
                    || maxWriteStreams <= 0 || ioThreads <= 0
                    || maxQueuedOperations < maxWriteStreams) {
                throw new IllegalArgumentException("invalid shuffle batch limits");
            }
        }
    }
}
