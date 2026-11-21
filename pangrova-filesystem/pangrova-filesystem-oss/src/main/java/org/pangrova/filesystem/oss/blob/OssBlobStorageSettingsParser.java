package org.pangrova.filesystem.oss.blob;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

/** Parses explicit FILE OSS settings without accepting AWS configuration aliases or credentials in YAML. */
public final class OssBlobStorageSettingsParser {
    private static final long CONNECT_TIMEOUT_MILLIS = 5000;
    private static final long READ_TIMEOUT_MILLIS = 20000;

    private OssBlobStorageSettingsParser() {}

    /** Returns validated OSS settings from the Worker function configuration. */
    public static OssBlobStorage.Settings parse(Map<String, String> config) {
        return new OssBlobStorage.Settings(required(config, "bucket"), required(config, "region"),
                URI.create(required(config, "endpoint")),
                config.getOrDefault("file.storage.oss.prefix", "pangrova/ai-files"),
                new OssBlobStorage.Encryption(required(config, "sse"), config.get("file.storage.oss.kms-key-id")),
                bool(config.getOrDefault("file.storage.oss.path-style-access", "false")),
                timeout(config, "connect-timeout-ms", CONNECT_TIMEOUT_MILLIS),
                timeout(config, "read-timeout-ms", READ_TIMEOUT_MILLIS));
    }

    private static String required(Map<String, String> config, String name) {
        String key = "file.storage.oss." + name;
        String value = config.get(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException(key + " is required");
        return value.trim();
    }

    private static boolean bool(String value) {
        if ("true".equalsIgnoreCase(value)) return true;
        if ("false".equalsIgnoreCase(value)) return false;
        throw new IllegalArgumentException("file.storage.oss.path-style-access must be true or false");
    }

    private static Duration timeout(Map<String, String> config, String name, long defaultMillis) {
        String key = "file.storage.oss." + name;
        try {
            return Duration.ofMillis(Long.parseLong(config.getOrDefault(key, Long.toString(defaultMillis))));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(key + " must be an integer", failure);
        }
    }
}
