package org.pangrova.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AiFileOptionsTest {

    @Test
    void productionIdentityAndStorageOptionsHaveNoDefaults() {
        assertThat(AiFileOptions.STORAGE_BACKEND.hasDefaultValue()).isFalse();
        assertThat(AiFileOptions.STORAGE_STAGING_ROOT.hasDefaultValue()).isFalse();
        assertThat(AiFileOptions.CAPABILITY_SECRET_REFERENCE.hasDefaultValue()).isFalse();
        assertThat(AiFileOptions.S3_BUCKET.hasDefaultValue()).isFalse();
        assertThat(AiFileOptions.S3_REGION.hasDefaultValue()).isFalse();
        assertThat(AiFileOptions.S3_SSE.hasDefaultValue()).isFalse();
    }

    @Test
    void boundedTransportAndStorageDefaultsAreTyped() {
        assertThat(AiFileOptions.S3_PREFIX.defaultValue()).isEqualTo("pangrova/ai-files");
        assertThat(AiFileOptions.S3_PATH_STYLE_ACCESS.defaultValue()).isFalse();
        assertThat(AiFileOptions.MAX_BYTES.defaultValue()).isEqualTo(64L * 1024 * 1024);
        assertThat(AiFileOptions.URL_MAX_CONNECTIONS.defaultValue()).isEqualTo(256);
        assertThat(AiFileOptions.URL_MAX_CONNECTIONS_PER_ROUTE.defaultValue()).isEqualTo(32);
        assertThat(AiFileOptions.URL_CONNECTION_TTL_MS.defaultValue()).isEqualTo(60_000L);
        assertThat(AiFileOptions.URL_IDLE_EVICTION_MS.defaultValue()).isEqualTo(30_000L);
    }
}
