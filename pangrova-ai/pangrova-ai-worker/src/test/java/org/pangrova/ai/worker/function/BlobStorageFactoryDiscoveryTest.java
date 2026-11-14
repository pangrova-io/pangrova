package org.pangrova.ai.worker.function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.filesystem.api.blob.BlobStorage;
import org.pangrova.filesystem.api.blob.BlobStorageFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlobStorageFactoryDiscoveryTest {
    @Test
    void resolvesTheThreeInstalledFactoriesAndValidatesOnlyTheSelectedConfiguration(@TempDir Path root)
            throws Exception {
        var providers = ServiceLoader.load(BlobStorageFactory.class, AiFunctionFactory.class.getClassLoader())
                .stream().map(ServiceLoader.Provider::get).toList();
        assertThat(providers).extracting(BlobStorageFactory::providerId)
                .containsExactlyInAnyOrder("local", "s3", "oss");
        try (var storage = AiFunctionFactory.createStorage(Map.of("file.storage-backend", "local",
                "file.storage.local.root", root.resolve("objects").toString()), providers)) {
            assertThat(storage).isNotNull();
            assertThat(root.resolve("objects")).isDirectory();
        }
        for (String id : List.of("s3", "oss")) {
            assertThatThrownBy(() -> AiFunctionFactory.createStorage(Map.of("file.storage-backend", id), providers))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("file.storage." + id);
        }
    }

    @Test
    void invalidInventoryDoesNotOpenAnyStorage() {
        var factory = new TestFactory("local", BlobStorageFactory.API_VERSION);
        var configuration = Map.of("file.storage-backend", "local");
        assertThatThrownBy(() -> AiFunctionFactory.createStorage(configuration, List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported");
        assertThatThrownBy(() -> AiFunctionFactory.createStorage(configuration, List.of(factory, factory)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate");
        assertThatThrownBy(() -> AiFunctionFactory.createStorage(configuration,
                List.of(factory, new TestFactory("other", BlobStorageFactory.API_VERSION + 1))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Invalid");
    }

    private record TestFactory(String providerId, int apiVersion) implements BlobStorageFactory {
        @Override
        public BlobStorage create(Map<String, String> configuration) {
            throw new AssertionError("Invalid inventory must be rejected before opening storage");
        }
    }
}
