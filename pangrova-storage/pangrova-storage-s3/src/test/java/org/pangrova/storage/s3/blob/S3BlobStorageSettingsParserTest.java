package org.pangrova.storage.s3.blob;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3BlobStorageSettingsParserTest {

    @Test
    void parsesCompleteFailClosedConfiguration() {
        Map<String, String> config = new HashMap<>();
        config.put("file.storage.s3.bucket", "governed-files");
        config.put("file.storage.s3.region", "us-east-1");
        config.put("file.storage.s3.endpoint", "https://storage.example/");
        config.put("file.storage.s3.prefix", "production/files");
        config.put("file.storage.s3.sse", "aws:kms");
        config.put("file.storage.s3.kms-key-id",
                "arn:aws:kms:us-east-1:123456789012:key/uuid");
        config.put("file.storage.s3.expected-bucket-owner", "123456789012");
        config.put("file.storage.s3.path-style-access", "true");

        S3BlobStorage.Settings settings = S3BlobStorageSettingsParser.parse(config);

        assertThat(settings.bucket()).isEqualTo("governed-files");
        assertThat(settings.region()).isEqualTo("us-east-1");
        assertThat(settings.endpoint()).isEqualTo(URI.create("https://storage.example"));
        assertThat(settings.prefix()).isEqualTo("production/files");
        assertThat(settings.encryption().mode())
                .isEqualTo(ServerSideEncryption.AWS_KMS);
        assertThat(settings.expectedBucketOwner()).isEqualTo("123456789012");
        assertThat(settings.pathStyleAccess()).isTrue();
    }

    @Test
    void rejectsMissingPolicyAndInvalidBoolean() {
        assertThatThrownBy(() -> S3BlobStorageSettingsParser.parse(Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file.storage.s3.sse is required");

        Map<String, String> config = new HashMap<>();
        config.put("file.storage.s3.bucket", "governed-files");
        config.put("file.storage.s3.region", "us-east-1");
        config.put("file.storage.s3.sse", "AES256");
        config.put("file.storage.s3.path-style-access", "sometimes");
        assertThatThrownBy(() -> S3BlobStorageSettingsParser.parse(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be true or false");
    }
}
