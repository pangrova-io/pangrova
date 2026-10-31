package org.pangrova.file.api.profile;

import org.junit.jupiter.api.Test;
import org.pangrova.common.storage.StorageType;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StorageProfileTest {

    private static StorageProfile profile(StorageType type, String prefix) {
        return new StorageProfile("source", type, "bucket", prefix, "", "", Map.of(), Map.of());
    }

    @Test
    void matchesOnlyTheConfiguredSchemeBucketAndPathBoundary() {
        StorageProfile profile = profile(StorageType.S3, "data");

        assertThat(profile.matchesUrl("s3://bucket/data/file.parquet")).isTrue();
        assertThat(profile.matchesUrl("s3://bucket/data")).isTrue();
        assertThat(profile.matchesUrl("s3://bucket/data-private/file.parquet")).isFalse();
        assertThat(profile.matchesUrl("oss://bucket/data/file.parquet")).isFalse();
        assertThat(profile.matchesUrl("s3://other/data/file.parquet")).isFalse();
    }

    @Test
    void rejectsAmbiguousAndTraversingObjectUrls() {
        StorageProfile profile = profile(StorageType.S3, "data/");

        assertThat(profile.matchesUrl("s3://bucket/data/file.parquet")).isTrue();
        assertThat(profile.matchesUrl("s3://bucket/data")).isFalse();
        assertThat(profile.matchesUrl("s3://bucket/data/../private/file.parquet")).isFalse();
        assertThat(profile.matchesUrl("s3://bucket/data/%2e%2e/private/file.parquet")).isFalse();
        assertThat(profile.matchesUrl("s3://bucket/data//file.parquet")).isFalse();
        assertThat(profile.matchesUrl("s3://bucket/data/file.parquet?version=2")).isFalse();
        assertThat(profile.matchesUrl("s3://user@bucket/data/file.parquet")).isFalse();
        assertThat(profile.matchesUrl("s3://bucket/data/file.parquet#part")).isFalse();
    }

    @Test
    void localProfileDoesNotAuthorizeWithoutCanonicalFilesystemResolution() {
        StorageProfile profile = profile(StorageType.LOCAL, "data");

        assertThat(profile.matchesUrl("file://bucket/data/file.parquet")).isFalse();
        assertThat(profile.isKeyWithinPrefix("data/file.parquet")).isTrue();
        assertThat(profile.isKeyWithinPrefix("data-private/file.parquet")).isFalse();
    }

    @Test
    void malformedScopeNeverExpandsToTheWholeBucket() {
        assertThat(profile(StorageType.S3, "/").matchesUrl("s3://bucket/private/file.parquet"))
                .isFalse();
        assertThat(profile(StorageType.S3, "data/../private")
                .matchesUrl("s3://bucket/private/file.parquet")).isFalse();
    }

    @Test
    void eachObjectBackendRequiresItsOwnCanonicalScheme() {
        assertThat(profile(StorageType.OSS, "data").matchesUrl("oss://bucket/data/a.parquet"))
                .isTrue();
        assertThat(profile(StorageType.GCS, "data").matchesUrl("gs://bucket/data/a.parquet"))
                .isTrue();
        assertThat(profile(StorageType.ADLS, "data")
                .matchesUrl("abfss://bucket/data/a.parquet")).isTrue();
        assertThat(profile(StorageType.OSS, "data").matchesUrl("s3://bucket/data/a.parquet"))
                .isFalse();
    }
}
