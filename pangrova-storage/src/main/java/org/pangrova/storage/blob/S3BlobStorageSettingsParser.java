package org.pangrova.storage.blob;

import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.net.URI;
import java.util.Map;

/** Parses the fail-closed Worker configuration for durable FILE storage. */
public final class S3BlobStorageSettingsParser {

    private S3BlobStorageSettingsParser() {}

    /** Returns validated S3 settings from the AI function configuration map. */
    public static S3BlobStorage.Settings parse(Map<String, String> config) {
        String encryptionName = required(config, "file.storage.s3.sse");
        ServerSideEncryption encryption = switch (encryptionName) {
            case "AES256" -> ServerSideEncryption.AES256;
            case "aws:kms" -> ServerSideEncryption.AWS_KMS;
            default -> throw new IllegalArgumentException(
                    "file.storage.s3.sse must be AES256 or aws:kms");
        };
        String endpoint = config.getOrDefault("file.storage.s3.endpoint", "").trim();
        return new S3BlobStorage.Settings(
                required(config, "file.storage.s3.bucket"),
                required(config, "file.storage.s3.region"),
                endpoint.isEmpty() ? null : URI.create(endpoint),
                config.getOrDefault("file.storage.s3.prefix", "pangrova/ai-files"),
                new S3BlobStorage.Encryption(
                        encryption, config.get("file.storage.s3.kms-key-id")),
                config.getOrDefault("file.storage.s3.expected-bucket-owner", ""),
                booleanValue(config, "file.storage.s3.path-style-access", false));
    }

    private static String required(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " is required");
        }
        return value.trim();
    }

    private static boolean booleanValue(
            Map<String, String> config, String key, boolean defaultValue) {
        String value = config.get(key);
        if (value == null) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException(key + " must be true or false");
    }
}
