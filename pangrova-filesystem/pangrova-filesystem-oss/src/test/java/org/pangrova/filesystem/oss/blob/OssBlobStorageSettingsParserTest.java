package org.pangrova.filesystem.oss.blob;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OssBlobStorageSettingsParserTest {
    @Test
    void requiresExplicitOssSettingsAndDoesNotReadAwsAliases() {
        var config = valid();
        var settings = OssBlobStorageSettingsParser.parse(config);
        assertThat(settings.bucket()).isEqualTo("test-bucket");
        assertThat(settings.encryption().algorithm()).isEqualTo("AES256");
        assertThat(settings.pathStyleAccess()).isFalse();
        for (String key : java.util.List.of("bucket", "region", "endpoint", "sse")) {
            var missing = new HashMap<>(config);
            missing.put("file.storage.s3." + key, missing.remove("file.storage.oss." + key));
            assertThatThrownBy(() -> OssBlobStorageSettingsParser.parse(missing))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("file.storage.oss." + key);
        }
    }

    @Test
    void rejectsNonHttpsOriginsUnsafePrefixesAndMalformedTransportPolicy() {
        for (var entry : java.util.List.of(
                Map.entry("endpoint", "http://localhost"), Map.entry("endpoint", "https://user:secret@example.test"),
                Map.entry("endpoint", "https://example.test/path"), Map.entry("endpoint", "https://example.test?key=x"),
                Map.entry("endpoint", "https://example.test#fragment"), Map.entry("prefix", "../escape"),
                Map.entry("prefix", "/root"), Map.entry("prefix", "a//b"),
                Map.entry("path-style-access", "yes"), Map.entry("connect-timeout-ms", "0"),
                Map.entry("read-timeout-ms", "invalid"), Map.entry("read-timeout-ms", "2147483648"))) {
            var config = valid();
            config.put("file.storage.oss." + entry.getKey(), entry.getValue());
            assertThatThrownBy(() -> OssBlobStorageSettingsParser.parse(config))
                    .as("invalid setting %s", entry).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void requiresExactKmsKeyAndRejectsEncryptionFromAnotherProvider() {
        for (String encryption : java.util.List.of("", "aws:kms", "SM4", "KMS")) {
            var config = valid();
            config.put("file.storage.oss.sse", encryption);
            assertThatThrownBy(() -> OssBlobStorageSettingsParser.parse(config))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        var config = valid();
        config.put("file.storage.oss.kms-key-id", "key-id");
        assertThatThrownBy(() -> OssBlobStorageSettingsParser.parse(config)).isInstanceOf(IllegalArgumentException.class);
        config.put("file.storage.oss.sse", "KMS");
        assertThat(OssBlobStorageSettingsParser.parse(config).encryption().kmsKeyId()).isEqualTo("key-id");
    }

    private static HashMap<String, String> valid() {
        return new HashMap<>(Map.of("file.storage.oss.bucket", "test-bucket", "file.storage.oss.region", "cn-hangzhou",
                "file.storage.oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com", "file.storage.oss.sse", "AES256"));
    }
}
