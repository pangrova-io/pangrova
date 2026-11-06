package org.pangrova.testing.testkit.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.file.api.error.FileStorageException;
import org.pangrova.common.storage.StorageType;
import org.pangrova.filesystem.api.profile.StorageProfile;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryStorageProfileRegistryTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void uniqueLongestMatchWinsDespiteShorterTies(boolean specificFirst) {
        var registry = new InMemoryStorageProfileRegistry();
        var specific = profile("specific", "data/nested/");
        if (specificFirst) registry.register(specific);
        registry.register(profile("broad-a", "data/"));
        registry.register(profile("broad-b", "data/"));
        if (!specificFirst) registry.register(specific);

        assertThat(registry.resolveByUrl("s3://bucket-a/data/nested/file.parquet")).contains(specific);
        assertThat(registry.resolveByUrl("s3://missing/data/nested/file.parquet")).isEmpty();
    }

    @Test
    void tiedLongestMatchesAreRejectedInsteadOfChoosingAnArbitraryProfile() {
        var registry = new InMemoryStorageProfileRegistry();
        registry.register(profile("broad", ""));
        registry.register(profile("first", "data/"));
        registry.register(profile("second", "data/"));

        assertThatThrownBy(() -> registry.resolveByUrl("s3://bucket-a/data/file.parquet"))
                .isInstanceOf(FileStorageException.class).hasMessageContaining("Ambiguous");
    }

    private static StorageProfile profile(String name, String prefix) {
        return new StorageProfile(name, StorageType.S3, "bucket-a", prefix, "", "", Map.of(), Map.of());
    }
}
