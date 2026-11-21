package org.pangrova.config.constants;

import org.pangrova.config.ConfigOption;
import org.pangrova.config.ConfigOptions;

/** Worker configuration options for governed durable FILE storage and import. */
public final class AiFileOptions {

    private AiFileOptions() {}

    public static final ConfigOption<String> STORAGE_BACKEND = stringOption(
            "worker.ai.file.storage-backend", null,
            "Required governed FILE production storage backend");
    public static final ConfigOption<String> STORAGE_STAGING_ROOT = stringOption(
            "worker.ai.file.storage-staging-root", null,
            "Worker-local governed FILE staging directory");
    public static final ConfigOption<String> LOCAL_ROOT = stringOption(
            "worker.ai.file.storage.local.root", null,
            "Persistent administrator-controlled governed FILE filesystem root");
    public static final ConfigOption<String> CAPABILITY_SECRET_REFERENCE = stringOption(
            "worker.ai.file.capability-secret-reference", null,
            "External reference for the shared FILE capability signing key");
    public static final ConfigOption<String> S3_BUCKET = stringOption(
            "worker.ai.file.storage.s3.bucket", null, "Governed FILE S3 bucket");
    public static final ConfigOption<String> S3_REGION = stringOption(
            "worker.ai.file.storage.s3.region", null, "Governed FILE S3 region");
    public static final ConfigOption<String> S3_ENDPOINT = stringOption(
            "worker.ai.file.storage.s3.endpoint", "", "S3-compatible HTTPS origin");
    public static final ConfigOption<String> S3_PREFIX = stringOption(
            "worker.ai.file.storage.s3.prefix", "pangrova/ai-files",
            "Tenant-partitioned governed FILE object prefix");
    public static final ConfigOption<String> S3_SSE = stringOption(
            "worker.ai.file.storage.s3.sse", null, "Required S3 server-side encryption");
    public static final ConfigOption<String> S3_KMS_KEY_ID = stringOption(
            "worker.ai.file.storage.s3.kms-key-id", "", "Expected S3 KMS key id or ARN");
    public static final ConfigOption<String> S3_EXPECTED_BUCKET_OWNER = stringOption(
            "worker.ai.file.storage.s3.expected-bucket-owner", "",
            "Expected twelve-digit S3 bucket owner");
    public static final ConfigOption<Boolean> S3_PATH_STYLE_ACCESS =
            ConfigOptions.key("worker.ai.file.storage.s3.path-style-access")
            .booleanType().defaultValue(false)
            .withDescription("Use path-style S3-compatible addressing").build();
    public static final ConfigOption<Long> MAX_BYTES = longOption(
            "worker.ai.file.max-bytes", 64L * 1024 * 1024,
            "Maximum decoded bytes accepted for one governed FILE");
    public static final ConfigOption<String> OSS_BUCKET = stringOption(
            "worker.ai.file.storage.oss.bucket", null, "Governed FILE OSS never-versioned bucket");
    public static final ConfigOption<String> OSS_REGION = stringOption(
            "worker.ai.file.storage.oss.region", null, "Governed FILE OSS signing region");
    public static final ConfigOption<String> OSS_ENDPOINT = stringOption(
            "worker.ai.file.storage.oss.endpoint", null, "Explicit OSS HTTPS origin");
    public static final ConfigOption<String> OSS_PREFIX = stringOption(
            "worker.ai.file.storage.oss.prefix", "pangrova/ai-files", "Tenant-partitioned FILE OSS object prefix");
    public static final ConfigOption<String> OSS_SSE = stringOption(
            "worker.ai.file.storage.oss.sse", null, "Required OSS server-side encryption: AES256 or KMS");
    public static final ConfigOption<String> OSS_KMS_KEY_ID = stringOption(
            "worker.ai.file.storage.oss.kms-key-id", "", "Exact OSS KMS key id");
    public static final ConfigOption<Boolean> OSS_PATH_STYLE_ACCESS =
            ConfigOptions.key("worker.ai.file.storage.oss.path-style-access").booleanType().defaultValue(false)
                    .withDescription("Use path-style OSS addressing").build();
    public static final ConfigOption<Long> OSS_CONNECT_TIMEOUT_MS = longOption(
            "worker.ai.file.storage.oss.connect-timeout-ms", 5_000L, "OSS connection timeout in milliseconds");
    public static final ConfigOption<Long> OSS_READ_TIMEOUT_MS = longOption(
            "worker.ai.file.storage.oss.read-timeout-ms", 20_000L, "OSS socket read timeout in milliseconds");
    public static final ConfigOption<String> ALLOWED_MIME_TYPES = stringOption(
            "worker.ai.file.allowed-mime-types",
            "image/jpeg,image/png,image/webp,application/pdf,text/plain,"
                    + "audio/mpeg,audio/wav,video/mp4",
            "Comma-separated exact MIME allowlist for governed FILE content");
    public static final ConfigOption<String> URL_ALLOWED_HOSTS = stringOption(
            "worker.ai.file.url.allowed-hosts", "",
            "Comma-separated exact DNS host allowlist for FILE_FROM_URL");
    public static final ConfigOption<Long> URL_CONNECT_TIMEOUT_MS = longOption(
            "worker.ai.file.url.connect-timeout-ms", 5_000L,
            "FILE_FROM_URL HTTPS connection deadline in milliseconds");
    public static final ConfigOption<Long> URL_READ_TIMEOUT_MS = longOption(
            "worker.ai.file.url.read-timeout-ms", 30_000L,
            "FILE_FROM_URL total response deadline in milliseconds");
    public static final ConfigOption<Long> URL_MAX_WIRE_BYTES = longOption(
            "worker.ai.file.url.max-wire-bytes", 64L * 1024 * 1024,
            "Maximum compressed bytes downloaded by FILE_FROM_URL");
    public static final ConfigOption<Long> URL_MAX_DECODED_BYTES = longOption(
            "worker.ai.file.url.max-decoded-bytes", 64L * 1024 * 1024,
            "Maximum decompressed bytes persisted by FILE_FROM_URL");
    public static final ConfigOption<Integer> URL_MAX_CONNECTIONS = integerOption(
            "worker.ai.file.url.max-connections", 256,
            "Maximum pooled FILE import connections");
    public static final ConfigOption<Integer> URL_MAX_CONNECTIONS_PER_ROUTE = integerOption(
            "worker.ai.file.url.max-connections-per-route", 32,
            "Maximum pooled FILE import connections per origin");
    public static final ConfigOption<Long> URL_CONNECTION_TTL_MS = longOption(
            "worker.ai.file.url.connection-ttl-ms", 60_000L,
            "FILE import pooled connection lifetime in milliseconds");
    public static final ConfigOption<Long> URL_IDLE_EVICTION_MS = longOption(
            "worker.ai.file.url.idle-eviction-ms", 30_000L,
            "FILE import idle connection eviction age in milliseconds");

    private static ConfigOption<String> stringOption(
            String key, String defaultValue, String description) {
        var builder = ConfigOptions.key(key).stringType();
        if (defaultValue != null) {
            builder.defaultValue(defaultValue);
        }
        return builder.withDescription(description).build();
    }

    private static ConfigOption<Long> longOption(
            String key, long defaultValue, String description) {
        return ConfigOptions.key(key).longType().defaultValue(defaultValue)
                .withDescription(description).build();
    }

    private static ConfigOption<Integer> integerOption(
            String key, int defaultValue, String description) {
        return ConfigOptions.key(key).intType().defaultValue(defaultValue)
                .withDescription(description).build();
    }
}
